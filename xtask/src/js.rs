use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::apple::verify_apple_xcframework;
use crate::support::{Result, copy_dir, repo_command, repo_root, run_command};

pub(crate) fn npm_publish(mode: &str) -> Result<()> {
    if mode != "dry-run" && mode != "publish" {
        return Err("usage: xtask npm-publish <dry-run|publish>".into());
    }

    let root = repo_root();
    let pnpm_cache = env::var_os("PNPM_CONFIG_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("target/pnpm-cache"));
    fs::create_dir_all(&pnpm_cache)?;

    let packages = publishable_npm_packages()?;
    if packages.is_empty() {
        println!("::notice title=No npm packages::No publishable npm packages were found.");
        return Ok(());
    }

    if packages
        .iter()
        .any(|package| package.name == "takanawa-capacitor")
    {
        prepare_capacitor_npm_package()?;
    }

    for package in &packages {
        let build_script = if mode == "publish" && package.name == "takanawa-node" {
            "build:ts"
        } else {
            "build"
        };
        println!("::group::pnpm --filter {} {build_script}", package.name);
        run_command(pnpm_command(&pnpm_cache).args([
            "--filter",
            package.name.as_str(),
            build_script,
        ]))?;
        println!("::endgroup::");
    }

    for package in &packages {
        if mode == "dry-run" {
            println!("::group::pnpm pack --dry-run {}", package.dir);
            run_command(
                pnpm_command(&pnpm_cache)
                    .current_dir(root.join(&package.dir))
                    .args(["pack", "--dry-run"]),
            )?;
            println!("::endgroup::");
            continue;
        }

        if npm_package_version_exists(&pnpm_cache, &package.name, &package.version)? {
            println!(
                "::notice title=Already published::{} {} already exists on npm; skipping.",
                package.name, package.version
            );
            continue;
        }

        let mut args = vec!["publish", "--provenance", "--no-git-checks"];
        if package.name.starts_with('@') {
            args.push("--access");
            args.push("public");
        }
        println!("::group::pnpm publish {}@{}", package.name, package.version);
        run_command(
            pnpm_command(&pnpm_cache)
                .current_dir(root.join(&package.dir))
                .args(args),
        )?;
        println!("::endgroup::");
    }

    Ok(())
}

fn pnpm_command(pnpm_cache: &Path) -> Command {
    let mut command = repo_command("pnpm");
    command.env("PNPM_CONFIG_CACHE_DIR", pnpm_cache);
    command
}

fn npm_package_version_exists(pnpm_cache: &Path, name: &str, version: &str) -> Result<bool> {
    let status = pnpm_command(pnpm_cache)
        .args(["view", format!("{name}@{version}").as_str(), "version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    Ok(status.success())
}

#[derive(Debug)]
struct NpmPackage {
    dir: String,
    name: String,
    version: String,
}

fn publishable_npm_packages() -> Result<Vec<NpmPackage>> {
    let packages_dir = repo_root().join("packages");
    if !packages_dir.is_dir() {
        return Ok(Vec::new());
    }

    let mut manifests = Vec::new();
    for entry in fs::read_dir(packages_dir)? {
        let entry = entry?;
        let manifest = entry.path().join("package.json");
        if manifest.is_file() {
            manifests.push(manifest);
        }
    }
    manifests.sort();

    let mut packages = Vec::new();
    for manifest_path in manifests {
        let manifest_text = fs::read_to_string(&manifest_path)?;
        let manifest: serde_json::Value = serde_json::from_str(&manifest_text)?;
        let dir = manifest_path
            .parent()
            .expect("package manifest should have a parent")
            .strip_prefix(repo_root())?
            .to_string_lossy()
            .replace('\\', "/");

        if manifest
            .get("private")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            println!("::notice title=Skipping private package::{dir}");
            continue;
        }

        let name = manifest
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("{dir}/package.json is missing name"))?
            .to_owned();
        let version = manifest
            .get("version")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("{dir}/package.json is missing version"))?
            .to_owned();
        packages.push(NpmPackage { dir, name, version });
    }

    Ok(packages)
}

fn prepare_capacitor_npm_package() -> Result<()> {
    let root = repo_root();
    let package_xcframework = root.join("packages/takanawa-capacitor/ios/Takanawa.xcframework");
    let package_takanawa_source = root.join("packages/takanawa-capacitor/ios/Sources/Takanawa");
    let swiftpm_zip = root.join("target/swiftpm/Takanawa.xcframework.zip");
    let local_xcframework = root.join("target/apple/Takanawa.xcframework");

    if package_xcframework.is_dir() {
        fs::remove_dir_all(&package_xcframework)?;
    }
    if package_takanawa_source.is_dir() {
        fs::remove_dir_all(&package_takanawa_source)?;
    }

    if swiftpm_zip.is_file() {
        run_command(
            repo_command("unzip")
                .args(["-q", "-o"])
                .arg(&swiftpm_zip)
                .arg("-d")
                .arg(root.join("packages/takanawa-capacitor/ios")),
        )?;
    } else if local_xcframework.is_dir() {
        copy_dir(&local_xcframework, &package_xcframework)?;
    } else {
        return Err(
            "missing Takanawa.xcframework for takanawa-capacitor; download the Apple artifact or run mise run package:apple first"
                .into(),
        );
    }

    if !package_xcframework.is_dir() {
        return Err("ios/Takanawa.xcframework was not staged for takanawa-capacitor".into());
    }
    verify_apple_xcframework(&package_xcframework)?;

    copy_dir(&root.join("Sources/Takanawa"), &package_takanawa_source)?;

    println!(
        "::notice title=Staged Capacitor XCFramework::{}",
        package_xcframework.display()
    );
    Ok(())
}
