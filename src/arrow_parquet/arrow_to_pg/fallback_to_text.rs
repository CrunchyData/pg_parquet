use arrow::array::{Array, StringArray};

use crate::type_compat::fallback_to_text::FallbackToText;

use super::{ArrowArrayToPgType, ArrowToPgContext};

// Text representation of any type
impl ArrowArrayToPgType<'_, StringArray, FallbackToText> for FallbackToText {
    fn to_pg_type(arr: StringArray, context: ArrowToPgContext<'_>) -> Option<FallbackToText> {
        if arr.is_null(0) {
            None
        } else {
            let text_repr = arr.value(0).to_string();
            let val = FallbackToText::new(text_repr, context.typoid, context.typmod);
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
        context: ArrowToPgContext<'_>,
    ) -> Option<Vec<Option<FallbackToText>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val =
                val.map(|val| FallbackToText::new(val.to_string(), context.typoid, context.typmod));
            vals.push(val);
        }
        Some(vals)
    }
}
