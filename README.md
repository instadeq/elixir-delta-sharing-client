# DeltaSharing

Elixir client library for the [Delta Sharing Protocol](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md)

## Status

Early stages of development, already useful

## Library Structure

There are two main modules: 

- `DeltaSharing.RawClient`: does the requests and returns the raw HTTP responses
- `DeltaSharing.Client`: does the requests and parses the responses returning Elixir structs


## Sample Usage

Download [open-datasets.share](https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share) or any other profile file you have access to

```elixir
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
```

## Installation

```elixir
def deps do
  [
    {:delta_sharing, "~> 0.1.0"}
  ]
end
```

## Docs

[hexdocs.pm/delta_sharing](https://hexdocs.pm/delta_sharing)

## License

MIT

