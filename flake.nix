{
  description = "Housekeeper - ClickHouse schema migration tool";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachSystem
      [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ]
      (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };

          # Pick language/tool versions here (adjust as you like)
          go = pkgs.go_1_25;
          python = pkgs.python314;

          # Common build utils
          buildUtils = with pkgs; [
            go-task
            golangci-lint
            goreleaser
            mkdocs
            python313Packages.mkdocs-material
            python313Packages.pymdown-extensions
          ];

          # Derive version from git
          # When there are no tags, this will create a version like:
          # - If clean: 0.0.0+a5b28d8 (short commit hash)
          # - If dirty: 0.0.0+a5b28d8-dirty
          # Once you add tags, it will use them: v1.0.0, v1.0.0-5-ga5b28d8, etc.
          version =
            if (self ? rev) then
              "0.0.0+${builtins.substring 0 7 self.rev}"
            else if (self ? dirtyRev) then
              "0.0.0+${builtins.substring 0 7 self.dirtyRev}-dirty"
            else
              "0.0.0+unknown";

          # Build the housekeeper binary
          housekeeper = pkgs.buildGoModule rec {
            pname = "housekeeper";
            inherit version;

            src = ./.;

            # This will need to be updated when dependencies change
            # Run `nix build .#housekeeper` and it will tell you the correct hash
            vendorHash = "sha256-vduBXmWFi1RJLsbouyjuZKX4d3Gwk+OEDxN+JADOYmM=";

            env.CGO_ENABLED = "0";
            env.GOPROXY = "direct";
            env.GOSUMDB = "off";

            ldflags = [
              "-s"
              "-w"
              "-X main.version=${version}"
              "-X main.commit=${if (self ? rev) then self.rev else if (self ? dirtyRev) then self.dirtyRev else "unknown"}"
              "-X main.date=${self.lastModifiedDate or "1970-01-01T00:00:00Z"}"
            ];

            # The main package is in cmd/housekeeper
            subPackages = [ "cmd/housekeeper" ];

            meta = with pkgs.lib; {
              description = "ClickHouse schema migration tool";
              homepage = "https://github.com/pseudomuto/housekeeper";
              license = licenses.gpl3Only;
              maintainers = [ {
                github = "pseudomuto";
                githubId = 159586;
                name = "David Muto";
              } ];
            };
          };

        in
        {
          # Make the package available for installation
          packages = {
            default = housekeeper;
            housekeeper = housekeeper;
          };

          # Make it runnable with `nix run`
          apps = {
            default = flake-utils.lib.mkApp {
              drv = housekeeper;
            };
            housekeeper = flake-utils.lib.mkApp {
              drv = housekeeper;
            };
          };

          # `nix develop` drops you into this shell
          devShells.default = pkgs.mkShell {
            packages = [
              go
              python
              buildUtils
            ];

            CGO_ENABLED = "0";

            # Helpful prompt when you enter the shell
            shellHook = ''
              echo "▶ Dev shell ready on ${system}"
              echo "   Go:    $(${go}/bin/go version)"
              echo "   Python: $(${python}/bin/python -V)"
            '';
          };

          # `nix fmt` to format nix files in this repo
          formatter = pkgs.nixfmt-tree;
        }
      );
}
