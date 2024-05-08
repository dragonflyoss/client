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

use std::env;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    // Set the environment variables for the cargo package version.
    println!("cargo:rustc-env=GIT_VERSION={}", env!("CARGO_PKG_VERSION"));

    // Set the environment variables for the git commit.
    if let Ok(output) = Command::new("git").args(["rev-parse", "HEAD"]).output() {
        if let Ok(commit) = String::from_utf8(output.stdout) {
            println!("cargo:rustc-env=GIT_COMMIT={}", commit.trim());
        }
    }

    // Set the environment variables for the build platform.
    if let Ok(target) = env::var("TARGET") {
        println!("cargo:rustc-env=BUILD_PLATFORM={}", target);
    }

    // Set the environment variables for the build time.
    if let Ok(build_time) = SystemTime::now().duration_since(UNIX_EPOCH) {
        println!("cargo:rustc-env=BUILD_TIMESTAMP={}", build_time.as_secs());
    }
}
