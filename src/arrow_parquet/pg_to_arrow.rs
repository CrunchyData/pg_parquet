use arrow::{array::ArrayRef, datatypes::FieldRef};
use arrow_schema::Fields;
use pgrx::{
    datum::{Date, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone, UnboxDatum},
    heap_tuple::PgHeapTuple,
    pg_sys::{
        Oid, BOOLOID, BYTEAOID, CHAROID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID,
        NUMERICOID, OIDOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID,
    },
    AllocatedByRust, AnyNumeric, FromDatum, IntoDatum, PgTupleDesc,
};

use crate::{
    pgrx_utils::{
        array_element_typoid, collect_valid_attributes, domain_array_base_elem_typoid,
        is_array_type, is_composite_type, tuple_desc,
    },
    type_compat::{
        fallback_to_text::{reset_fallback_to_text_context, FallbackToText},
        geometry::{is_postgis_geometry_typoid, Geometry},
        map::{is_crunchy_map_typoid, CrunchyMap},
        pg_arrow_type_conversions::{
            extract_precision_from_numeric_typmod, extract_scale_from_numeric_typmod,
            MAX_DECIMAL_PRECISION,
        },
    },
};

pub(crate) mod bool;
pub(crate) mod bytea;
pub(crate) mod char;
pub(crate) mod date;
pub(crate) mod fallback_to_text;
pub(crate) mod float4;
pub(crate) mod float8;
pub(crate) mod geometry;
pub(crate) mod int2;
pub(crate) mod int4;
pub(crate) mod int8;
pub(crate) mod map;
pub(crate) mod numeric;
pub(crate) mod oid;
pub(crate) mod record;
pub(crate) mod text;
pub(crate) mod time;
pub(crate) mod timestamp;
pub(crate) mod timestamptz;
pub(crate) mod timetz;

pub(crate) trait PgTypeToArrowArray<T: IntoDatum + FromDatum + UnboxDatum> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef;
}

#[derive(Clone)]
pub(crate) struct PgToArrowAttributeContext {
    name: String,
    field: FieldRef,
    typoid: Oid,
    typmod: i32,
    is_array: bool,
    is_composite: bool,
    is_geometry: bool,
    is_crunchy_map: bool,
    attribute_contexts: Option<Vec<PgToArrowAttributeContext>>,
    scale: Option<usize>,
    precision: Option<usize>,
}

impl PgToArrowAttributeContext {
    fn new(name: String, typoid: Oid, typmod: i32, fields: Fields) -> Self {
        let field = fields
            .iter()
            .find(|field| field.name() == &name)
            .unwrap()
            .clone();

        let is_array = is_array_type(typoid);
        let is_composite;
        let is_geometry;
        let is_crunchy_map;
        let attribute_typoid;
        let attribute_field;

        if is_array {
            let element_typoid = array_element_typoid(typoid);

            is_composite = is_composite_type(element_typoid);
            is_geometry = is_postgis_geometry_typoid(element_typoid);
            is_crunchy_map = is_crunchy_map_typoid(element_typoid);

            if is_crunchy_map {
                let entries_typoid = domain_array_base_elem_typoid(element_typoid);
                attribute_typoid = entries_typoid;
            } else {
                attribute_typoid = element_typoid;
            }

            attribute_field = match field.data_type() {
                arrow::datatypes::DataType::List(field) => field.clone(),
                _ => unreachable!(),
            }
        } else {
            is_composite = is_composite_type(typoid);
            is_geometry = is_postgis_geometry_typoid(typoid);
            is_crunchy_map = is_crunchy_map_typoid(typoid);

            if is_crunchy_map {
                let entries_typoid = domain_array_base_elem_typoid(typoid);
                attribute_typoid = entries_typoid;
            } else {
                attribute_typoid = typoid;
            }

            attribute_field = field.clone();
        }

        let attribute_tupledesc = if is_composite || is_crunchy_map {
            Some(tuple_desc(attribute_typoid, typmod))
        } else {
            None
        };

        let precision;
        let scale;
        if attribute_typoid == NUMERICOID {
            precision = Some(extract_precision_from_numeric_typmod(typmod));
            scale = Some(extract_scale_from_numeric_typmod(typmod));
        } else {
            precision = None;
            scale = None;
        }

        // for composite and crunchy_map types, recursively collect attribute contexts
        let attribute_contexts = attribute_tupledesc.map(|attribute_tupledesc| {
            let fields = match attribute_field.data_type() {
                arrow::datatypes::DataType::Struct(fields) => fields.clone(),
                arrow::datatypes::DataType::Map(struct_field, _) => {
                    match struct_field.data_type() {
                        arrow::datatypes::DataType::Struct(fields) => fields.clone(),
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            };

            collect_attribute_contexts(&attribute_tupledesc, &fields)
        });

        Self {
            name,
            field: attribute_field,
            typoid: attribute_typoid,
            typmod,
            is_array,
            is_composite,
            is_geometry,
            is_crunchy_map,
            attribute_contexts,
            scale,
            precision,
        }
    }
}

pub(crate) fn collect_attribute_contexts(
    tupledesc: &PgTupleDesc,
    fields: &Fields,
) -> Vec<PgToArrowAttributeContext> {
    let include_generated_columns = true;
    let attributes = collect_valid_attributes(tupledesc, include_generated_columns);
    let mut attribute_contexts = vec![];

    for attribute in attributes {
        let attribute_name = attribute.name();
        let attribute_typoid = attribute.type_oid().value();
        let attribute_typmod = attribute.type_mod();

        let attribute_context = PgToArrowAttributeContext::new(
            attribute_name.to_string(),
            attribute_typoid,
            attribute_typmod,
            fields.clone(),
        );

        attribute_contexts.push(attribute_context);
    }

    attribute_contexts
}

pub(crate) fn to_arrow_array(
    tuple: &PgHeapTuple<AllocatedByRust>,
    attribute_context: &PgToArrowAttributeContext,
) -> ArrayRef {
    if attribute_context.is_array {
        to_arrow_list_array(tuple, attribute_context)
    } else {
        to_arrow_primitive_array(tuple, attribute_context)
    }
}

fn to_arrow_primitive_array(
    tuple: &PgHeapTuple<AllocatedByRust>,
    attribute_context: &PgToArrowAttributeContext,
) -> ArrayRef {
    match attribute_context.typoid {
        FLOAT4OID => {
            let attribute_val: Option<f32> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        FLOAT8OID => {
            let attribute_val: Option<f64> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        INT2OID => {
            let attribute_val: Option<i16> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        INT4OID => {
            let attribute_val: Option<i32> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        INT8OID => {
            let attribute_val: Option<i64> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(attribute_context.typmod);

            if precision > MAX_DECIMAL_PRECISION {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let attribute_val: Option<FallbackToText> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            } else {
                let attribute_val: Option<AnyNumeric> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            }
        }
        BOOLOID => {
            let attribute_val: Option<bool> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        DATEOID => {
            let attribute_val: Option<Date> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        TIMEOID => {
            let attribute_val: Option<Time> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        TIMETZOID => {
            let attribute_val: Option<TimeWithTimeZone> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        TIMESTAMPOID => {
            let attribute_val: Option<Timestamp> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        TIMESTAMPTZOID => {
            let attribute_val: Option<TimestampWithTimeZone> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        CHAROID => {
            let attribute_val: Option<i8> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        TEXTOID => {
            let attribute_val: Option<String> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        BYTEAOID => {
            let attribute_val: Option<&[u8]> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        OIDOID => {
            let attribute_val: Option<Oid> = tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        _ => {
            if attribute_context.is_composite {
                let attribute_val: Option<PgHeapTuple<AllocatedByRust>> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            } else if attribute_context.is_crunchy_map {
                let attribute_val: Option<CrunchyMap> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            } else if attribute_context.is_geometry {
                let attribute_val: Option<Geometry> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            } else {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let attribute_val: Option<FallbackToText> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            }
        }
    }
}

pub(crate) fn to_arrow_list_array(
    tuple: &PgHeapTuple<AllocatedByRust>,
    attribute_context: &PgToArrowAttributeContext,
) -> ArrayRef {
    match attribute_context.typoid {
        FLOAT4OID => {
            let attribute_val: Option<pgrx::Array<f32>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        FLOAT8OID => {
            let attribute_val: Option<pgrx::Array<f64>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        INT2OID => {
            let attribute_val: Option<pgrx::Array<i16>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        INT4OID => {
            let attribute_val: Option<pgrx::Array<i32>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        INT8OID => {
            let attribute_val: Option<pgrx::Array<i64>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        NUMERICOID => {
            let precision = attribute_context.precision.unwrap();

            if precision > MAX_DECIMAL_PRECISION {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let attribute_val: Option<pgrx::Array<FallbackToText>> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            } else {
                let attribute_val: Option<pgrx::Array<AnyNumeric>> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            }
        }
        BOOLOID => {
            let attribute_val: Option<pgrx::Array<bool>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        DATEOID => {
            let attribute_val: Option<pgrx::Array<Date>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        TIMEOID => {
            let attribute_val: Option<pgrx::Array<Time>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        TIMETZOID => {
            let attribute_val: Option<pgrx::Array<TimeWithTimeZone>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        TIMESTAMPOID => {
            let attribute_val: Option<pgrx::Array<Timestamp>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        TIMESTAMPTZOID => {
            let attribute_val: Option<pgrx::Array<TimestampWithTimeZone>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        CHAROID => {
            let attribute_val: Option<pgrx::Array<i8>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        TEXTOID => {
            let attribute_val: Option<pgrx::Array<String>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        BYTEAOID => {
            let attribute_val: Option<pgrx::Array<&[u8]>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        OIDOID => {
            let attribute_val: Option<pgrx::Array<Oid>> =
                tuple.get_by_name(&attribute_context.name).unwrap();

            attribute_val.to_arrow_array(attribute_context)
        }
        _ => {
            if attribute_context.is_composite {
                let attribute_val: Option<pgrx::Array<PgHeapTuple<AllocatedByRust>>> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            } else if attribute_context.is_crunchy_map {
                let attribute_val: Option<pgrx::Array<CrunchyMap>> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            } else if attribute_context.is_geometry {
                let attribute_val: Option<pgrx::Array<Geometry>> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            } else {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let attribute_val: Option<pgrx::Array<FallbackToText>> =
                    tuple.get_by_name(&attribute_context.name).unwrap();

                attribute_val.to_arrow_array(attribute_context)
            }
        }
    }
}
