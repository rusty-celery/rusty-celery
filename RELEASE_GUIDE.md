# Publishing a new release

1. Set the environment variable `VERSION` to the desired version and change the version in all subcrate `Cargo.toml` files and in the root `Cargo.toml`. Also change the version of the subcrate dependencies in the root `Cargo.toml`. All versions should now match `VERSION`.

2. Update the [CHANGELOG]("CHANGELOG.md").

3. Commit these changes:

    ```bash
    git commit -a -m "(cargo-release) version $VERSION"
    ```

4. Add a tag in git to mark the release:

    ```bash
    git tag "v$VERSION" -m "Adds tag v$VERSION for cargo"
    ```

5. Push the tag to git:

    ```bash
    git push --tags origin master
    ```

6. For each subcrate and the root crate (do the root crate last), build and publish to [crates.io](crates.io) by running `cargo publish` within the corresponding directory.

7. Lastly, go to the corresponding release on GitHub and copy over the notes from the CHANGELOG. Then add a "Commits" section with the output of this command:

    ```bash
    git log `git describe --always --tags --abbrev=0 HEAD^^`..HEAD^ --oneline
    ```

    Or, if you're using fish shell:

    ```fish
    git log (git describe --always --tags --abbrev=0 HEAD^^)..HEAD^ --oneline
    ```
