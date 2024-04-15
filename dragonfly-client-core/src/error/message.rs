/*
 *     Copyright 2024 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::borrow::Cow;

// Message is the message for the error.
#[derive(Debug)]
pub struct Message(Cow<'static, str>);

// From<&'static str> for Message implements the conversion from &'static str to Message.
impl From<&'static str> for Message {
    // from returns the message from the string.
    fn from(s: &'static str) -> Self {
        Message(Cow::Borrowed(s))
    }
}

// From<String> for Message implements the conversion from String to Message.
impl From<String> for Message {
    // from returns the message from the string.
    fn from(s: String) -> Self {
        Message(Cow::Owned(s))
    }
}

// Message implements the message for the error.
impl Message {
    // as_str returns the string of the message.
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
