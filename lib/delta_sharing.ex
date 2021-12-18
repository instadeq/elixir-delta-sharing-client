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
  end

  defmodule Client do
    alias DeltaSharing.{Profile, RawClient, Response}

    def new(profile) do
      middleware = [
        {Tesla.Middleware.BaseUrl, profile.endpoint},
        Tesla.Middleware.JSON,
        {Tesla.Middleware.Headers, [{"Authorization", "Bearer " <> profile.bearerToken}]}
      ]

      adapter = {Tesla.Adapter.Mint, [recv_timeout: 30_000]}

      Tesla.client(middleware, adapter)
    end

    defp with_ok(resp, fun) do
      case resp do
        {:ok, env} ->
          fun.(env)

        error ->
          error
      end
    end

    defp with_ok_and_body(resp, fun) do
      case resp do
        {:ok, %{status: status, body: body}} ->
          if status == 200 do
            fun.(body)
          else
            {:error, %{reason: :bad_status, status: status}}
          end

        error ->
          error
      end
    end

    def list_shares(client, max_results \\ nil, page_token \\ nil) do
      resp = RawClient.list_shares(client, max_results, page_token)

      with_ok_and_body(resp, fn body ->
        {:ok, Response.Shares.from_data(body)}
      end)
    end

    def get_share(client, share) do
      resp = RawClient.get_share(client, share)

      with_ok_and_body(resp, fn body ->
        {:ok, Response.Shares.Share.from_data(Map.get(body, "share", %{}))}
      end)
    end

    def list_schemas_in_share(client, share, max_results \\ nil, page_token \\ nil) do
      resp = RawClient.list_schemas_in_share(client, share, max_results, page_token)

      with_ok_and_body(resp, fn body ->
        {:ok, Response.Schemas.from_data(body)}
      end)
    end

    def list_tables_in_schemas(client, share, schema, max_results \\ nil, page_token \\ nil) do
      resp = RawClient.list_tables_in_schemas(client, share, schema, max_results, page_token)

      with_ok_and_body(resp, fn body ->
        {:ok, Response.Tables.from_data(body)}
      end)
    end

    def list_all_tables_in_share(client, share, max_results \\ nil, page_token \\ nil) do
      resp = RawClient.list_all_tables_in_share(client, share, max_results, page_token)

      with_ok_and_body(resp, fn body ->
        {:ok, Response.Tables.from_data(body)}
      end)
    end

    def query_table_version(client, share, schema, table) do
      resp = RawClient.query_table_version(client, share, schema, table)

      with_ok(resp, fn %{headers: headers} ->
        version = :proplists.get_value("delta-table-version", headers, nil)
        {:ok, version}
      end)
    end

    defp parse_header_body_lines(body, first_parser, rest_parser) do
      [first_line | lines] = String.split(body, "\n", trim: true)

      first_struct = first_parser.(first_line)

      rest_structs =
        for line <- lines do
          rest_parser.(line)
        end

      {first_struct, rest_structs}
    end

    defp parse_2_headers_body_lines(body, first_parser, second_parser, rest_parser) do
      [first_line, second_line | lines] = String.split(body, "\n", trim: true)

      first_struct = first_parser.(first_line)
      second_struct = second_parser.(second_line)

      rest_structs =
        for line <- lines do
          rest_parser.(line)
        end

      {first_struct, second_struct, rest_structs}
    end

    def query_table_metadata(client, share, schema, table) do
      resp = RawClient.query_table_metadata(client, share, schema, table)

      with_ok_and_body(resp, fn body ->
        {protocol, [metadata]} =
          parse_header_body_lines(
            body,
            &Response.Protocol.from_table_metadata_json/1,
            &Response.MetaData.Table.from_table_metadata_json/1
          )

        {:ok, Response.TableMeta.new(protocol, metadata)}
      end)
    end

    def query_table(client, share, schema, table, limit_hint \\ nil, predicate_hints \\ nil) do
      resp = RawClient.query_table(client, share, schema, table, limit_hint, predicate_hints)

      with_ok_and_body(resp, fn body ->
        {protocol, metadata, files} =
          parse_2_headers_body_lines(
            body,
            &Response.Protocol.from_table_metadata_json/1,
            &Response.MetaData.Table.from_table_metadata_json/1,
            &Response.Query.File.from_query_table_json/1
          )

        {:ok, Response.Query.new(protocol, metadata, files)}
      end)
    end
  end

  defmodule RawClient do
    alias DeltaSharing.Profile

    defp remove_nil_values(plist) do
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

    defp maybe_set_key(map, key, val) do
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
    alias DeltaSharing.{Profile, Client, RawClient}
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

    Client.list_shares(c)
    Client.get_share(c, "delta_sharing")
    Client.list_schemas_in_share(c, "delta_sharing")
    Client.list_tables_in_schemas(c, "delta_sharing", "default")
    Client.list_all_tables_in_share(c, "delta_sharing")
    Client.query_table_version(c, "delta_sharing", "default", "COVID_19_NYT")
    Client.query_table_metadata(c, "delta_sharing", "default", "COVID_19_NYT")
    Client.query_table(c, "delta_sharing", "default", "COVID_19_NYT", 10)
  end
end
