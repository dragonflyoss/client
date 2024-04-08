use std::borrow::Cow;

#[derive(Debug)]
pub struct Message(Cow<'static, str>);

impl From<&'static str> for Message {
    fn from(s: &'static str) -> Self {
        Message(Cow::Borrowed(s))
    }
}

impl From<String> for Message {
    fn from(s: String) -> Self {
        Message(Cow::Owned(s))
    }
}

impl Message {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message() {
        let message: Message = "hello".into();
        assert_eq!(message.as_str(), "hello");

        let message: Message = "world".to_string().into();
        assert_eq!(message.as_str(), "world");
    }
}