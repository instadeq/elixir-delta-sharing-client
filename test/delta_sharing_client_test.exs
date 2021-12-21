defmodule DeltaSharing.RawClientTest do
  use ExUnit.Case
  alias DeltaSharing.{Profile, RawClient, Client}

  defmodule FixedResponseAdapter do
    @behaviour Tesla.Adapter

    @impl Tesla.Adapter
    def call(_env, opts) do
      %{resp: resp} = opts
      resp
    end
  end

  def response_client(resp) do
    adapter = {FixedResponseAdapter, %{resp: resp}}
    profile = Profile.new("http://localhost:9875", "bad_json")
    client = Client.new(profile, adapter)

    client
  end

  @no_json_body "I'm not json"
  def bad_json_client() do
    status_client(200, @no_json_body)
  end

  def status_client(status, body, headers \\ []) do
    resp = {:ok, %Tesla.Env{status: status, body: body, headers: headers}}
    response_client(resp)
  end

  test "list_shares handles bad json" do
    client = bad_json_client()
    {:ok, %{status: 200, body: @no_json_body}} = RawClient.list_shares(client)
    {:error, %{reason: :bad_body}} = Client.list_shares(client)
  end

  test "list_shares handles non 200 status" do
    client = status_client(404, %{})
    {:ok, %{status: 404, body: %{}}} = RawClient.list_shares(client)
    {:error, %{reason: :bad_status}} = Client.list_shares(client)
  end

  test "get_share handles bad json" do
    client = bad_json_client()
    {:ok, %{status: 200, body: @no_json_body}} = RawClient.get_share(client, "share1")
    {:error, %{reason: :bad_body}} = Client.get_share(client, "share1")
  end

  test "get_share handles non 200 status" do
    client = status_client(404, %{})
    {:ok, %{status: 404, body: %{}}} = RawClient.get_share(client, "share1")
    {:error, %{reason: :bad_status}} = Client.get_share(client, "share1")
  end

  test "list_schemas_in_share handles bad json" do
    client = bad_json_client()
    {:ok, %{status: 200, body: @no_json_body}} = RawClient.list_schemas_in_share(client, "share1")
    {:error, %{reason: :bad_body}} = Client.list_schemas_in_share(client, "share1")
  end

  test "list_schemas_in_share handles non 200 status" do
    client = status_client(404, %{})
    {:ok, %{status: 404, body: %{}}} = RawClient.list_schemas_in_share(client, "share1")
    {:error, %{reason: :bad_status}} = Client.list_schemas_in_share(client, "share1")
  end

  test "list_tables_in_schemas handles bad json" do
    client = bad_json_client()

    {:ok, %{status: 200, body: @no_json_body}} =
      RawClient.list_tables_in_schemas(client, "share1", "schema1")

    {:error, %{reason: :bad_body}} = Client.list_tables_in_schemas(client, "share1", "schema1")
  end

  test "list_tables_in_schemas handles non 200 status" do
    client = status_client(404, %{})

    {:ok, %{status: 404, body: %{}}} =
      RawClient.list_tables_in_schemas(client, "share1", "schema1")

    {:error, %{reason: :bad_status}} = Client.list_tables_in_schemas(client, "share1", "schema1")
  end

  test "list_all_tables_in_share handles bad json" do
    client = bad_json_client()

    {:ok, %{status: 200, body: @no_json_body}} =
      RawClient.list_all_tables_in_share(client, "share1")

    {:error, %{reason: :bad_body}} = Client.list_all_tables_in_share(client, "share1")
  end

  test "list_all_tables_in_share handles non 200 status" do
    client = status_client(404, %{})
    {:ok, %{status: 404, body: %{}}} = RawClient.list_all_tables_in_share(client, "share1")
    {:error, %{reason: :bad_status}} = Client.list_all_tables_in_share(client, "share1")
  end

  test "query_table_version handles missing header in response" do
    client = status_client(200, "", [])

    {:ok, %{status: 200, body: "", headers: []}} =
      RawClient.query_table_version(client, "share1", "schema1", "table1")

    {:ok, nil} = Client.query_table_version(client, "share1", "schema1", "table1")
  end

  test "query_table_version handles non 200 status" do
    client = status_client(404, "", [])

    {:ok, %{status: 404, body: "", headers: []}} =
      RawClient.query_table_version(client, "share1", "schema1", "table1")

    {:error, %{reason: :bad_status}} =
      Client.query_table_version(client, "share1", "schema1", "table1")
  end

  test "query_table_version handles header in response" do
    client = status_client(200, "", [{"delta-table-version", 42}])
    {:ok, 42} = Client.query_table_version(client, "share1", "schema1", "table1")
  end

  test "query_table handles non 200 status" do
    client = status_client(404, "")

    {:ok, %{status: 404, body: ""}} = RawClient.query_table(client, "share1", "schema1", "table1")

    {:error, %{reason: :bad_status}} = Client.query_table(client, "share1", "schema1", "table1")
  end
end
