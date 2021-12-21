defmodule DeltaSharingTest do
  use ExUnit.Case
  alias DeltaSharing.Response.{Shares, Schemas, Protocol, MetaData, Tables}

  test "Shares.from_data" do
    data = %{"items" => [%{"name" => "delta_sharing"}]}
    expected = %Shares{nextPageToken: nil, items: [%Shares.Share{id: nil, name: "delta_sharing"}]}
    ^expected = Shares.from_data(data)

    data1 = %{"nextPageToken" => "t1", "items" => [%{"name" => "delta_sharing"}]}

    expected1 = %Shares{
      nextPageToken: "t1",
      items: [%Shares.Share{id: nil, name: "delta_sharing"}]
    }

    ^expected1 = Shares.from_data(data1)
  end

  test "Share.from_data" do
    data = %{"name" => "delta_sharing"}
    expected = %Shares.Share{id: nil, name: "delta_sharing"}
    ^expected = Shares.Share.from_data(data)

    data1 = %{"name" => "delta_sharing", "id" => "id1"}
    expected1 = %Shares.Share{id: "id1", name: "delta_sharing"}
    ^expected1 = Shares.Share.from_data(data1)
  end

  test "Schemas.from_data" do
    data = %{"items" => [%{"name" => "delta_sharing"}]}

    expected = %Schemas{
      share: "share1",
      nextPageToken: nil,
      items: [%Schemas.Schema{id: nil, name: "delta_sharing"}]
    }

    ^expected = Schemas.from_data("share1", data)

    data1 = %{"nextPageToken" => "t1", "items" => [%{"name" => "delta_sharing"}]}

    expected1 = %Schemas{
      share: "share1",
      nextPageToken: "t1",
      items: [%Schemas.Schema{id: nil, name: "delta_sharing"}]
    }

    ^expected1 = Schemas.from_data("share1", data1)
  end

  test "Schema.from_data" do
    data = %{"name" => "delta_sharing"}
    expected = %Schemas.Schema{id: nil, name: "delta_sharing"}
    ^expected = Schemas.Schema.from_data(data)

    data1 = %{"name" => "delta_sharing", "id" => "id1"}
    expected1 = %Schemas.Schema{id: "id1", name: "delta_sharing"}
    ^expected1 = Schemas.Schema.from_data(data1)
  end

  test "Tables.from_data" do
    data = %{
      "items" => [
        %{
          "id" => "id1",
          "name" => "COVID_19_NYT",
          "schema" => "default",
          "share" => "delta_sharing"
        },
        %{
          "name" => "owid-covid-data",
          "schema" => "default",
          "share" => "delta_sharing"
        }
      ]
    }

    expected = %Tables{
      share: "share1",
      schema: "schema1",
      items: [
        %Tables.Table{
          id: "id1",
          name: "COVID_19_NYT",
          schema: "default",
          share: "delta_sharing",
          shareId: nil
        },
        %Tables.Table{
          id: nil,
          name: "owid-covid-data",
          schema: "default",
          share: "delta_sharing",
          shareId: nil
        }
      ],
      nextPageToken: nil
    }

    ^expected = Tables.from_data("share1", "schema1", data)
  end

  test "Protocol.from_table_metadata_json" do
    json = "{\"protocol\":{\"minReaderVersion\":1}}"
    expected = %Protocol{minReaderVersion: 1}
    ^expected = Protocol.from_table_metadata_json(json)
  end

  test "MetaData.Table.from_table_metadata_json" do
    json =
      "{\"metaData\":{\"id\":\"7245fd1d-8a6d-4988-af72-92a95b646511\",\"format\":{\"provider\":\"parquet\"},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"date\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"county\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"state\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"fips\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"cases\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"deaths\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[\"a\",\"b\"]}}"

    expected = %MetaData.Table{
      description: nil,
      format: %MetaData.Table.Format{provider: "parquet"},
      id: "7245fd1d-8a6d-4988-af72-92a95b646511",
      name: nil,
      partitionColumns: ["a", "b"],
      schemaString:
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"county\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"state\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fips\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cases\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"deaths\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"
    }

    ^expected = MetaData.Table.from_table_metadata_json(json)
  end
end
