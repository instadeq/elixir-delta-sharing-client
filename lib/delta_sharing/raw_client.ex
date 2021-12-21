defmodule DeltaSharing.RawClient do
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
