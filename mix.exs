defmodule RaxElasticlunr.MixProject do
  use Mix.Project

  def project do
    [
      app: :rax_elasticlunr,
      version: "0.1.0",
      elixir: "~> 1.13",
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

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:rax, git: "https://github.com/zambal/rax.git"},
      {:elasticlunr, "~> 0.6.4"}
    ]
  end
end
