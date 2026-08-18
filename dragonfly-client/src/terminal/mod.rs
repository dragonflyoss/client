/*
 *     Copyright 2026 The Dragonfly Authors
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

use std::fmt::Display;
use termion::{color, style};

/// The separator line between sections of command line output.
const SEPARATOR: &str = "****************************************";

/// Returns the text in bold italic with the given color.
fn styled(fg: impl color::Color, text: impl Display) -> String {
    format!(
        "{}{}{}{}{}",
        color::Fg(fg),
        style::Italic,
        style::Bold,
        text,
        style::Reset
    )
}

/// Prints a red headline, e.g. `Downloading Failed!`.
pub fn error(text: impl Display) {
    println!("{}", styled(color::Red, text));
}

/// Prints a green headline, e.g. `Task Removed!`.
pub fn success(text: impl Display) {
    println!("{}", styled(color::Green, text));
}

/// Prints a yellow warning message.
pub fn warn(text: impl Display) {
    println!("{}", styled(color::Yellow, text));
}

/// Prints the separator line between sections.
pub fn separator() {
    println!("{}", styled(color::Black, SEPARATOR));
}

/// Prints a cyan key followed by a plain value, e.g. `Message: ...`.
pub fn field(key: &str, value: impl Display) {
    println!("{} {}", styled(color::Cyan, key), value);
}

/// Prints a red key followed by a plain value, e.g. `Bad Code: ...`.
pub fn error_field(key: &str, value: impl Display) {
    println!("{} {}", styled(color::Red, key), value);
}

/// Prints a cyan `Header:` key followed by indented `[key]: value` lines.
pub fn headers<'a>(entries: impl IntoIterator<Item = (&'a str, &'a str)>) {
    println!("{}", styled(color::Cyan, "Header:"));
    for (key, value) in entries {
        println!("  [{key}]: {value}");
    }
}
