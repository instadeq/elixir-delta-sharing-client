defmodule DeltaSharing.Client do
  alias DeltaSharing.{RawClient, Response}

  def new(profile, adapter \\ {Tesla.Adapter.Mint, [recv_timeout: 30_000]}) do
    middleware = [
      {Tesla.Middleware.BaseUrl, profile.endpoint},
      Tesla.Middleware.JSON,
      {Tesla.Middleware.Headers, [{"Authorization", "Bearer " <> profile.bearerToken}]}
    ]

    Tesla.client(middleware, adapter)
  end

  defp with_ok(resp, fun) do
    case resp do
      {:ok, env = %{status: status}} ->
        if status == 200 do
          fun.(env)
        else
          {:error, %{reason: :bad_status, status: status}}
        end

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

  defp with_ok_and_map_body(resp, fun) do
    with_ok_and_body(resp, fn body ->
      if is_map(body) do
        fun.(body)
      else
        {:error, %{reason: :bad_body, body: body}}
      end
    end)
  end

  def list_shares(client, max_results \\ nil, page_token \\ nil) do
    resp = RawClient.list_shares(client, max_results, page_token)

    with_ok_and_map_body(resp, fn body ->
      {:ok, Response.Shares.from_data(body)}
    end)
  end

  def get_share(client, share) do
    resp = RawClient.get_share(client, share)

    with_ok_and_map_body(resp, fn body ->
      {:ok, Response.Shares.Share.from_data(Map.get(body, "share", %{}))}
    end)
  end

  def list_schemas_in_share(client, share, max_results \\ nil, page_token \\ nil) do
    resp = RawClient.list_schemas_in_share(client, share, max_results, page_token)

    with_ok_and_map_body(resp, fn body ->
      {:ok, Response.Schemas.from_data(body)}
    end)
  end

  def list_tables_in_schemas(client, share, schema, max_results \\ nil, page_token \\ nil) do
    resp = RawClient.list_tables_in_schemas(client, share, schema, max_results, page_token)

    with_ok_and_map_body(resp, fn body ->
      {:ok, Response.Tables.from_data(body)}
    end)
  end

  def list_all_tables_in_share(client, share, max_results \\ nil, page_token \\ nil) do
    resp = RawClient.list_all_tables_in_share(client, share, max_results, page_token)

    with_ok_and_map_body(resp, fn body ->
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
