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
    tuples: &Vec<Option<PgHeapTuple<AllocatedByRust>>>,
    attribute_context: &PgToArrowAttributeContext,
) -> ArrayRef {
    if attribute_context.is_array {
        to_arrow_list_array(tuples, attribute_context)
    } else {
        to_arrow_primitive_array(tuples, attribute_context)
    }
}

macro_rules! to_primitive_arrow_array {
    ($pg_type:ty, $tuples:expr, $attribute_context:expr) => {{
        let mut attribute_vals = vec![];

        for tuple in $tuples {
            pgrx::pg_sys::check_for_interrupts!();

            if let Some(tuple) = tuple {
                let attribute_val: Option<$pg_type> =
                    tuple.get_by_name(&$attribute_context.name).unwrap();
                attribute_vals.push(attribute_val);
            } else {
                attribute_vals.push(None);
            }
        }

        return attribute_vals.to_arrow_array($attribute_context);
    }};
}

macro_rules! to_list_arrow_array {
    ($pg_type:ty, $tuples:expr, $attribute_context:expr) => {{
        let mut attribute_vals = vec![];

        for tuple in $tuples {
            pgrx::pg_sys::check_for_interrupts!();

            if let Some(tuple) = tuple {
                let attribute_val: Option<$pg_type> =
                    tuple.get_by_name(&$attribute_context.name).unwrap();
                attribute_vals.push(attribute_val);
            } else {
                attribute_vals.push(None);
            }
        }

        let attribute_vals = attribute_vals
            .iter()
            .map(|val| val.as_ref().map(|val| val.iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        return attribute_vals.to_arrow_array($attribute_context);
    }};
}

fn to_arrow_primitive_array(
    tuples: &Vec<Option<PgHeapTuple<AllocatedByRust>>>,
    attribute_context: &PgToArrowAttributeContext,
) -> ArrayRef {
    match attribute_context.typoid {
        FLOAT4OID => to_primitive_arrow_array!(f32, tuples, attribute_context),
        FLOAT8OID => to_primitive_arrow_array!(f64, tuples, attribute_context),
        INT2OID => to_primitive_arrow_array!(i16, tuples, attribute_context),
        INT4OID => to_primitive_arrow_array!(i32, tuples, attribute_context),
        INT8OID => to_primitive_arrow_array!(i64, tuples, attribute_context),
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(attribute_context.typmod);

            if precision > MAX_DECIMAL_PRECISION {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                to_primitive_arrow_array!(FallbackToText, tuples, attribute_context)
            } else {
                to_primitive_arrow_array!(AnyNumeric, tuples, attribute_context)
            }
        }
        BOOLOID => to_primitive_arrow_array!(bool, tuples, attribute_context),
        DATEOID => to_primitive_arrow_array!(Date, tuples, attribute_context),
        TIMEOID => to_primitive_arrow_array!(Time, tuples, attribute_context),
        TIMETZOID => to_primitive_arrow_array!(TimeWithTimeZone, tuples, attribute_context),
        TIMESTAMPOID => to_primitive_arrow_array!(Timestamp, tuples, attribute_context),
        TIMESTAMPTZOID => {
            to_primitive_arrow_array!(TimestampWithTimeZone, tuples, attribute_context)
        }
        CHAROID => to_primitive_arrow_array!(i8, tuples, attribute_context),
        TEXTOID => to_primitive_arrow_array!(String, tuples, attribute_context),
        BYTEAOID => to_primitive_arrow_array!(&[u8], tuples, attribute_context),
        OIDOID => to_primitive_arrow_array!(Oid, tuples, attribute_context),
        _ => {
            if attribute_context.is_composite {
                let mut attribute_vals = vec![];

                let attribute_tupledesc =
                    tuple_desc(attribute_context.typoid, attribute_context.typmod);

                for tuple in tuples {
                    pgrx::pg_sys::check_for_interrupts!();

                    if let Some(tuple) = tuple {
                        let attribute_val: Option<PgHeapTuple<AllocatedByRust>> =
                            tuple.get_by_name(&attribute_context.name).unwrap();

                        // this trick is needed to avoid having a bunch of
                        // reference counted tupledesc which comes from pgrx's "get_by_name".
                        // we first convert PgHeapTuple into unsafe HeapTuple to drop
                        // the reference counted tupledesc and then convert it back to
                        // PgHeapTuple by reusing the same tupledesc that we created
                        // before the loop. Only overhead is 1 "heap_copy_tuple" call.
                        let attribute_val = attribute_val.map(|tuple| tuple.into_pg());
                        let attribute_val = attribute_val.map(|tuple| unsafe {
                            PgHeapTuple::from_heap_tuple(attribute_tupledesc.clone(), tuple)
                                .into_owned()
                        });

                        attribute_vals.push(attribute_val);
                    } else {
                        attribute_vals.push(None);
                    }
                }

                attribute_vals.to_arrow_array(attribute_context)
            } else if attribute_context.is_crunchy_map {
                to_primitive_arrow_array!(CrunchyMap, tuples, attribute_context)
            } else if attribute_context.is_geometry {
                to_primitive_arrow_array!(Geometry, tuples, attribute_context)
            } else {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                to_primitive_arrow_array!(FallbackToText, tuples, attribute_context)
            }
        }
    }
}

fn to_arrow_list_array(
    tuples: &Vec<Option<PgHeapTuple<AllocatedByRust>>>,
    attribute_context: &PgToArrowAttributeContext,
) -> ArrayRef {
    match attribute_context.typoid {
        FLOAT4OID => to_list_arrow_array!(pgrx::Array<f32>, tuples, attribute_context),
        FLOAT8OID => to_list_arrow_array!(pgrx::Array<f64>, tuples, attribute_context),
        INT2OID => to_list_arrow_array!(pgrx::Array<i16>, tuples, attribute_context),
        INT4OID => to_list_arrow_array!(pgrx::Array<i32>, tuples, attribute_context),
        INT8OID => to_list_arrow_array!(pgrx::Array<i64>, tuples, attribute_context),
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(attribute_context.typmod);

            if precision > MAX_DECIMAL_PRECISION {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                to_list_arrow_array!(pgrx::Array<FallbackToText>, tuples, attribute_context)
            } else {
                to_list_arrow_array!(pgrx::Array<AnyNumeric>, tuples, attribute_context)
            }
        }
        BOOLOID => to_list_arrow_array!(pgrx::Array<bool>, tuples, attribute_context),
        DATEOID => to_list_arrow_array!(pgrx::Array<Date>, tuples, attribute_context),
        TIMEOID => to_list_arrow_array!(pgrx::Array<Time>, tuples, attribute_context),
        TIMETZOID => {
            to_list_arrow_array!(pgrx::Array<TimeWithTimeZone>, tuples, attribute_context)
        }
        TIMESTAMPOID => {
            to_list_arrow_array!(pgrx::Array<Timestamp>, tuples, attribute_context)
        }
        TIMESTAMPTZOID => {
            to_list_arrow_array!(
                pgrx::Array<TimestampWithTimeZone>,
                tuples,
                attribute_context
            )
        }
        CHAROID => to_list_arrow_array!(pgrx::Array<i8>, tuples, attribute_context),
        TEXTOID => to_list_arrow_array!(pgrx::Array<String>, tuples, attribute_context),
        BYTEAOID => to_list_arrow_array!(pgrx::Array<&[u8]>, tuples, attribute_context),
        OIDOID => to_list_arrow_array!(pgrx::Array<Oid>, tuples, attribute_context),
        _ => {
            if attribute_context.is_composite {
                let mut attribute_vals = vec![];

                let attribute_tupledesc =
                    tuple_desc(attribute_context.typoid, attribute_context.typmod);

                for tuple in tuples {
                    pgrx::pg_sys::check_for_interrupts!();

                    if let Some(tuple) = tuple {
                        let attribute_val: Option<pgrx::Array<PgHeapTuple<AllocatedByRust>>> =
                            tuple.get_by_name(&attribute_context.name).unwrap();

                        if let Some(attribute_val) = attribute_val {
                            let attribute_val = attribute_val
                                .iter()
                                .map(|tuple| tuple.map(|tuple| tuple.into_pg()))
                                .collect::<Vec<_>>();

                            let attribute_val = attribute_val
                                .iter()
                                .map(|tuple| {
                                    tuple.map(|tuple| unsafe {
                                        PgHeapTuple::from_heap_tuple(
                                            attribute_tupledesc.clone(),
                                            tuple,
                                        )
                                        .into_owned()
                                    })
                                })
                                .collect::<Vec<_>>();

                            attribute_vals.push(Some(attribute_val));
                        } else {
                            attribute_vals.push(None);
                        }
                    } else {
                        attribute_vals.push(None);
                    }
                }

                attribute_vals.to_arrow_array(attribute_context)
            } else if attribute_context.is_crunchy_map {
                to_list_arrow_array!(pgrx::Array<CrunchyMap>, tuples, attribute_context)
            } else if attribute_context.is_geometry {
                to_list_arrow_array!(pgrx::Array<Geometry>, tuples, attribute_context)
            } else {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                to_list_arrow_array!(pgrx::Array<FallbackToText>, tuples, attribute_context)
            }
        }
    }
}
