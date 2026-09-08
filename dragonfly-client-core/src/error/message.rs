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

/// The message for the error.
#[derive(Debug)]
pub struct Message(Cow<'static, str>);

/// From<&'static str> for Message implements the conversion from &'static str to Message.
impl From<&'static str> for Message {
    /// Returns the message from the string.
    fn from(s: &'static str) -> Self {
        Message(Cow::Borrowed(s))
    }
}

/// From<String> for Message implements the conversion from String to Message.
impl From<String> for Message {
    /// Returns the message from the string.
    fn from(s: String) -> Self {
        Message(Cow::Owned(s))
    }
}

/// Implements the message for the error.
impl Message {
    /// Returns the string of the message.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_and_string_preserve_text() {
        let test_cases: Vec<(Message, &str)> = vec![
            ("hello".into(), "hello"),
            ("world".to_string().into(), "world"),
        ];

        for (message, expected) in test_cases {
            assert_eq!(message.as_str(), expected);
        }
    }
}
