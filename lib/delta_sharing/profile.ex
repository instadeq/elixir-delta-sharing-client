defmodule DeltaSharing.Profile do
  alias __MODULE__

  @derive {Inspect, except: [:bearerToken]}
  defstruct path: nil, shareCredentialsVersion: 1, endpoint: "", bearerToken: ""

  def new(endpoint, bearer_token, path \\ nil, share_credentials_version \\ 1) do
    %Profile{
      path: path,
      shareCredentialsVersion: share_credentials_version,
      endpoint: endpoint,
      bearerToken: bearer_token
    }
  end

  def from_file(path) do
    case File.read(path) do
      {:ok, binary} ->
        case Jason.decode(binary) do
          {:ok, data} ->
            case data do
              %{
                "shareCredentialsVersion" => share_credentials_version,
                "endpoint" => endpoint,
                "bearerToken" => bearer_token
              } ->
                new(endpoint, bearer_token, path, share_credentials_version)

              _other ->
                {:error, %{reason: :bad_profile_format, data: data}}
            end

          error ->
            error
        end

      error ->
        error
    end
  end
end
