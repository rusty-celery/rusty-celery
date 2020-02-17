# Publishing a new release

1. Change the version in all subcrate `Cargo.toml` files and in the root `Cargo.toml`. Also change the version of the subcrate dependencies in the root `Cargo.toml`. All versions should now match.
2. Update the [CHANGELOG]("CHANGELOG.md").
3. Commit these changes with the message `(cargo-release) version VERSION`.
4. Add a tag in git to mark the release: `git tag VERSION -m 'Adds tag VERSION for cargo'`.
5. Push the tag to git: `git push --tags origin master`.
6. For each subcrate and the root crate, build and publish to [crates.io](crates.io) by running `cargo publish` within the corresponding directory.
