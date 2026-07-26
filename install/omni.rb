# Homebrew formula for OMNI orchestration CLI + daemon
# Install: brew install mhkim315/tap/omni
class Omni < Formula
  desc "OMNI orchestration CLI and daemon for AI agent workflows"
  homepage "https://github.com/mhkim315/omni_orchestration"
  url "https://github.com/mhkim315/omni_orchestration/archive/refs/tags/v2.2.0.tar.gz"
  sha256 "PLACEHOLDER_SHA256_FROM_GITHUB_RELEASE"
  license "MIT"
  version "2.2.0"

  depends_on "go" => :build

  def install
    system "go", "build", "-o", bin/"omni", "./cmd/omni"
    system "go", "build", "-o", bin/"omnid", "./cmd/omnid"
  end

  test do
    system "#{bin}/omni", "doctor"
  end
end
