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

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Locates the libfabric installation and compiles the C shim for the optional `rdma`
/// feature. The default build (feature disabled) does not touch libfabric at all.
fn main() {
    if env::var_os("CARGO_FEATURE_RDMA").is_none() {
        return;
    }

    println!("cargo:rerun-if-changed=src/rdma/shim.c");
    println!("cargo:rerun-if-env-changed=LIBFABRIC_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=LIBFABRIC_LIB_DIR");

    let (include_dir, lib_dir) = locate_libfabric();
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is set by cargo"));
    let obj = out_dir.join("dfrdma_shim.o");
    let archive = out_dir.join("libdfrdma_shim.a");

    let cc = env::var("CC").unwrap_or_else(|_| "cc".to_string());
    let mut compile = Command::new(&cc);
    compile
        .arg("-c")
        .arg("src/rdma/shim.c")
        .arg("-o")
        .arg(&obj)
        .arg("-O2")
        .arg("-fPIC")
        .arg("-Wall")
        .arg("-Werror");
    if let Some(ref include_dir) = include_dir {
        compile.arg(format!("-I{}", include_dir.display()));
    }
    let status = compile
        .status()
        .expect("failed to run the C compiler; the rdma feature requires a C toolchain");
    assert!(status.success(), "failed to compile src/rdma/shim.c");

    let status = Command::new(env::var("AR").unwrap_or_else(|_| "ar".to_string()))
        .arg("crs")
        .arg(&archive)
        .arg(&obj)
        .status()
        .expect("failed to run ar");
    assert!(status.success(), "failed to archive the rdma shim");

    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=dfrdma_shim");
    emit_libfabric_link(lib_dir.as_deref());
}

/// Emits linker search paths and libraries for libfabric and its RDMA transitive
/// dependencies. Amazon's libfabric needs `libefa` and `libibverbs` (often installed
/// under `/usr` while libfabric lives in `/opt/amazon/efa/lib`); distro builds used for
/// software-provider CI typically resolve the same DT_NEEDED entries from the system path.
fn emit_libfabric_link(lib_dir: Option<&Path>) {
    if let Some(dir) = lib_dir {
        println!("cargo:rustc-link-search=native={}", dir.display());
        // Keep runtime resolution working when libfabric lives outside the default loader
        // path (for example `/opt/amazon/efa/lib` on AWS EFA nodes).
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dir.display());
    }
    println!("cargo:rustc-link-lib=dylib=fabric");

    let fabric_so = lib_dir.and_then(|dir| {
        ["libfabric.so", "libfabric.dylib"]
            .into_iter()
            .map(|name| dir.join(name))
            .find(|path| path.exists())
    });
    let needed = fabric_so
        .as_ref()
        .map(|path| read_needed_libs(path))
        .unwrap_or_default();

    // Always consider the Amazon EFA pair; also honor whatever else libfabric DT_NEEDED.
    let mut deps: Vec<String> = vec!["libefa.so.1".into(), "libibverbs.so.1".into()];
    for lib in needed {
        if (lib.starts_with("libefa.so") || lib.starts_with("libibverbs.so"))
            && !deps.iter().any(|d| d == &lib)
        {
            deps.push(lib);
        }
    }

    for soname in deps {
        let Some(dir) = find_library_dir(lib_dir, &soname) else {
            continue;
        };
        if Some(dir.as_path()) != lib_dir {
            println!("cargo:rustc-link-search=native={}", dir.display());
        }
        // Prefer the unversioned -lefa / -libverbs linker name when the development
        // symlink exists; otherwise link the exact soname (`-l:libefa.so.1`).
        let stem = soname
            .trim_start_matches("lib")
            .split(".so")
            .next()
            .unwrap_or("efa");
        if library_in_dir(&dir, stem) {
            println!("cargo:rustc-link-lib=dylib={stem}");
        } else {
            println!("cargo:rustc-link-arg=-l:{soname}");
        }
    }
}

/// Parses DT_NEEDED entries from a shared library via `readelf` (or `otool` is not needed;
/// macOS libfabric builds do not pull Amazon EFA deps).
fn read_needed_libs(lib: &Path) -> Vec<String> {
    let output = Command::new("readelf").args(["-d"]).arg(lib).output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if !line.contains("(NEEDED)") {
                return None;
            }
            let start = line.find('[')?;
            let end = line.find(']')?;
            Some(line[start + 1..end].to_string())
        })
        .collect()
}

/// Returns a directory that contains `soname` (e.g. `libefa.so.1`) or an unversioned
/// `lib{stem}.so` / `.a` / `.dylib`, preferring `preferred` when it has the library.
fn find_library_dir(preferred: Option<&Path>, soname: &str) -> Option<PathBuf> {
    let stem = soname
        .trim_start_matches("lib")
        .split(".so")
        .next()
        .unwrap_or(soname);

    let mut dirs = Vec::new();
    if let Some(dir) = preferred {
        dirs.push(dir.to_path_buf());
    }
    dirs.extend(
        [
            "/opt/amazon/efa/lib",
            "/usr/lib/x86_64-linux-gnu",
            "/lib/x86_64-linux-gnu",
            "/usr/lib64",
            "/usr/lib",
            "/usr/local/lib",
        ]
        .into_iter()
        .map(PathBuf::from),
    );

    dirs.into_iter()
        .find(|dir| dir.join(soname).exists() || library_in_dir(dir, stem))
}

fn library_in_dir(dir: &Path, name: &str) -> bool {
    [
        dir.join(format!("lib{name}.so")),
        dir.join(format!("lib{name}.a")),
        dir.join(format!("lib{name}.dylib")),
    ]
    .iter()
    .any(|path| path.exists())
}

/// Resolves libfabric include and library directories from, in order: explicit environment
/// variables, pkg-config, and well-known installation prefixes.
fn locate_libfabric() -> (Option<PathBuf>, Option<PathBuf>) {
    let env_include = env::var_os("LIBFABRIC_INCLUDE_DIR").map(PathBuf::from);
    let env_lib = env::var_os("LIBFABRIC_LIB_DIR").map(PathBuf::from);
    if env_include.is_some() || env_lib.is_some() {
        return (env_include, env_lib);
    }

    if let Ok(output) = Command::new("pkg-config")
        .args(["--cflags-only-I", "--libs-only-L", "libfabric"])
        .output()
    {
        if output.status.success() {
            let flags = String::from_utf8_lossy(&output.stdout);
            let include = flags
                .split_whitespace()
                .find_map(|flag| flag.strip_prefix("-I").map(PathBuf::from));
            let lib = flags
                .split_whitespace()
                .find_map(|flag| flag.strip_prefix("-L").map(PathBuf::from));
            if include.is_some() || lib.is_some() {
                return (include, lib);
            }
        }
    }

    for prefix in [
        "/opt/amazon/efa",
        "/opt/homebrew/opt/libfabric",
        "/usr/local",
        "/usr",
    ] {
        let prefix = PathBuf::from(prefix);
        if prefix.join("include/rdma/fabric.h").exists() {
            let lib64 = prefix.join("lib64");
            let lib = if lib64.exists() {
                lib64
            } else {
                prefix.join("lib")
            };
            return (Some(prefix.join("include")), Some(lib));
        }
    }

    panic!(
        "the rdma feature requires libfabric; install it (e.g. apt install libfabric-dev, \
         brew install libfabric, or the AWS EFA installer) or set LIBFABRIC_INCLUDE_DIR and \
         LIBFABRIC_LIB_DIR"
    );
}
