use cfg_aliases::cfg_aliases;

fn main() {
    cfg_aliases! {
        pre_pg18: { any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17") },
        pre_pg17: { any(feature = "pg14", feature = "pg15", feature = "pg16") },
        pre_pg16: { any(feature = "pg14", feature = "pg15")  },
        pre_pg15: { any(feature = "pg14")  },
    }
}
