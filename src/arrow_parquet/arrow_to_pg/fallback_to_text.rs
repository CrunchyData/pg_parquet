use arrow::array::{Array, StringArray};

use crate::type_compat::fallback_to_text::{reset_fallback_to_text_context, FallbackToText};

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Text representation of any type
impl ArrowArrayToPgType<'_, StringArray, FallbackToText> for FallbackToText {
    fn to_pg_type(
        arr: StringArray,
        context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<FallbackToText> {
        reset_fallback_to_text_context(context.typoid, context.typmod);

        if arr.is_null(0) {
            None
        } else {
            let text_repr = arr.value(0).to_string();
            let val = FallbackToText(text_repr);
            Some(val)
        }
    }
}

// Text[] representation of any type
impl ArrowArrayToPgType<'_, StringArray, Vec<Option<FallbackToText>>>
    for Vec<Option<FallbackToText>>
{
    fn to_pg_type(
        arr: StringArray,
        context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Vec<Option<FallbackToText>>> {
        reset_fallback_to_text_context(context.typoid, context.typmod);

        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| FallbackToText(val.to_string()));
            vals.push(val);
        }
        Some(vals)
    }
}
