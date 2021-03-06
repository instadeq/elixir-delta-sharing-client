# DeltaSharing

Elixir client library for the [Delta Sharing Protocol](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md) by the [instadeq](https://instadeq.com) team

## Status

Implements the whole protocol as of [this version](https://github.com/delta-io/delta-sharing/blob/dab37674a10aa8f74dbe2b958197c6d0f0872650/PROTOCOL.md)

## Library Structure

There are two main modules:

- `DeltaSharing.RawClient`: does the requests and returns the raw HTTP responses
- `DeltaSharing.Client`: does the requests and parses the responses returning Elixir structs


## Sample Usage

Download [open-datasets.share](https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share) or any other profile file you have access to

```elixir
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

Client.list_shares(c)
Client.get_share(c, "delta_sharing")
Client.list_schemas_in_share(c, "delta_sharing")
Client.list_tables_in_schemas(c, "delta_sharing", "default")

# pagination
{:ok, r1} = Client.list_tables_in_schemas(c, "delta_sharing", "default", 5)
Client.next(c, r1, 5)

Client.list_all_tables_in_share(c, "delta_sharing")

# pagination
{:ok, r2} = Client.list_all_tables_in_share(c, "delta_sharing", 5)
Client.next(c, r2, 5)

Client.query_table_version(c, "delta_sharing", "default", "COVID_19_NYT")
Client.query_table_metadata(c, "delta_sharing", "default", "COVID_19_NYT")
Client.query_table(c, "delta_sharing", "default", "COVID_19_NYT", 10)
```

## Installation

Follow Instructions on [hex.pm/packages/delta_sharing/](https://hex.pm/packages/delta_sharing/)

## Docs

[delta_sharing docs on Hex Docs](https://hexdocs.pm/delta_sharing)

## License

MIT

