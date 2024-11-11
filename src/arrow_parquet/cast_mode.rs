use std::str::FromStr;

#[derive(Debug, Copy, Clone)]
pub(crate) enum CastMode {
    Strict,
    Relaxed,
}

impl FromStr for CastMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "strict" => Ok(Self::Strict),
            "relaxed" => Ok(Self::Relaxed),
            _ => Err(format!(
                "{} is not a valid cast_mode. Set it to either 'strict' or 'relaxed'.",
                s
            )),
        }
    }
}
