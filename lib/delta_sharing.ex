defmodule DeltaSharing do
  defmodule Profile do
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

    def join_slash(prefix, suffix) do
      case String.ends_with?(prefix, "/") do
        true -> "#{prefix}#{suffix}"
        false -> "#{prefix}/#{suffix}"
      end
    end

    def format_list_shares_url(profile = %Profile{}) do
      join_slash(profile.endpoint, "shares")
    end
  end

  defmodule Client do
    alias DeltaSharing.{Profile, RawClient}

    def new(profile) do
      middleware = [
        {Tesla.Middleware.BaseUrl, profile.endpoint},
        Tesla.Middleware.JSON,
        {Tesla.Middleware.Headers, [{"Authorization", "Bearer " <> profile.bearerToken}]}
      ]

      adapter = {Tesla.Adapter.Mint, [recv_timeout: 30_000]}

      Tesla.client(middleware, adapter)
    end

    def query_table(client, share, schema, table, limit_hint \\ nil, predicate_hints \\ nil) do
      {:ok, %{body: body}} =
        RawClient.query_table(client, share, schema, table, limit_hint, predicate_hints)

      for line <- String.split(body, "\n", trim: true) do
        {:ok, data} = Jason.decode(line)
        data
      end
    end
  end

  defmodule RawClient do
    alias DeltaSharing.Profile

    def remove_nil_values(plist) do
      for {k, v} <- plist, v != nil do
        {k, v}
      end
    end

    def list_shares(client, max_results \\ nil, page_token \\ nil) do
      query = remove_nil_values(maxResults: max_results, pageToken: page_token)
      Tesla.get(client, "/shares", query: query)
    end

    def get_share(client, share) do
      Tesla.get(client, "/shares/#{share}")
    end

    def list_schemas_in_share(client, share, max_results \\ nil, page_token \\ nil) do
      query = remove_nil_values(maxResults: max_results, pageToken: page_token)
      Tesla.get(client, "/shares/#{share}/schemas", query: query)
    end

    def list_tables_in_schemas(client, share, schema, max_results \\ nil, page_token \\ nil) do
      query = remove_nil_values(maxResults: max_results, pageToken: page_token)
      Tesla.get(client, "/shares/#{share}/schemas/#{schema}/tables", query: query)
    end

    def list_all_tables_in_share(client, share, max_results \\ nil, page_token \\ nil) do
      query = remove_nil_values(maxResults: max_results, pageToken: page_token)
      Tesla.get(client, "/shares/#{share}/all-tables", query: query)
    end

    def query_table_version(client, share, schema, table) do
      Tesla.head(client, "/shares/#{share}/schemas/#{schema}/tables/#{table}")
    end

    def query_table_metadata(client, share, schema, table) do
      Tesla.get(client, "/shares/#{share}/schemas/#{schema}/tables/#{table}/metadata")
    end

    def maybe_set_key(map, key, val) do
      if val == nil do
        map
      else
        Map.put(map, key, val)
      end
    end

    def query_table(client, share, schema, table, limit_hint \\ nil, predicate_hints \\ nil) do
      url = "/shares/#{share}/schemas/#{schema}/tables/#{table}/query"

      body =
        %{}
        |> maybe_set_key("predicateHints", predicate_hints)
        |> maybe_set_key("limitHint", limit_hint)

      Tesla.post(client, url, body)
    end
  end

  def test() do
    p = Profile.from_file("../open-datasets.share")
    c = Client.new(p)
    RawClient.list_shares(c)
    RawClient.get_share(c, "delta_sharing")
    RawClient.list_schemas_in_share(c, "delta_sharing")
    RawClient.list_tables_in_schemas(c, "delta_sharing", "default")
    RawClient.list_all_tables_in_share(c, "delta_sharing")
    RawClient.query_table_version(c, "delta_sharing", "default", "COVID_19_NYT")
    RawClient.query_table_metadata(c, "delta_sharing", "default", "COVID_19_NYT")
    RawClient.query_table(c, "delta_sharing", "default", "COVID_19_NYT", 10)

    r = Client.query_table(c, "delta_sharing", "default", "COVID_19_NYT", 10)
    [_protocol, _metadata, %{"file" => %{"url" => url}} | _] = r
    Tesla.get(url)
  end
end
