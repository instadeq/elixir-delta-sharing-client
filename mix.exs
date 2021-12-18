defmodule DeltaSharing.MixProject do
  use Mix.Project

  @source_url "https://github.com/instadeq/elixir-delta-sharing-client"
  @version "0.2.1"

  def project do
    [
      app: :delta_sharing,
      version: @version,
      description: description(),
      package: package(),
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs()
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
      links: %{"GitHub" => @source_url}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:mint, "~> 1.4"},
      {:castore, "~> 0.1.14"},
      {:jason, "~> 1.2"},
      {:tesla, "~> 1.4"},
      {:ecto, "~> 3.7"}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      source_ref: "v#{@version}",
      extras: ["README.md", "LICENSE"],
      groups_for_modules: [
        Clients: [
          DeltaSharing.Client,
          DeltaSharing.RawClient
        ],
        Profile: [
          DeltaSharing.Profile
        ]
      ]
    ]
  end
end
