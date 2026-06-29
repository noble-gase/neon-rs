pub trait IntoStrVec {
    fn into_str_vec(self) -> Vec<String>;
}

impl IntoStrVec for String {
    fn into_str_vec(self) -> Vec<String> {
        vec![self]
    }
}

impl IntoStrVec for &str {
    fn into_str_vec(self) -> Vec<String> {
        vec![self.to_string()]
    }
}

impl<T> IntoStrVec for Vec<T>
where
    T: Into<String>,
{
    fn into_str_vec(self) -> Vec<String> {
        self.into_iter().map(Into::into).collect()
    }
}

impl<T, const N: usize> IntoStrVec for [T; N]
where
    T: Into<String>,
{
    fn into_str_vec(self) -> Vec<String> {
        self.into_iter().map(Into::into).collect()
    }
}
