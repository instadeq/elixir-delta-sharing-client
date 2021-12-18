defmodule DeltaSharing.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :delta_sharing,
      version: @version,
      description: description(),
      package: package(),
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp description do
    "Elixir Client for the Delta Sharing Protocol"
  end

  defp package do
    [
      maintainers: ["Mariano Guerra"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/instadeq/elixir-delta-sharing-client"}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mint, "~> 1.4"},
      {:castore, "~> 0.1.14"},
      {:jason, "~> 1.2"},
      {:tesla, "~> 1.4"}
    ]
  end
end
