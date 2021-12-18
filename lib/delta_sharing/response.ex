defmodule DeltaSharing.Response do
  defmodule Shares do
    alias __MODULE__
    use Ecto.Schema
    import Ecto.Changeset

    defmodule Share do
      alias __MODULE__
      use Ecto.Schema
      import Ecto.Changeset

      @primary_key false
      embedded_schema do
        field(:id, :string, default: nil)
        field(:name, :string)
      end

      def from_data(data) do
        %Share{}
        |> cast(data, [:id, :name])
        |> apply_changes()
      end

      def changeset(struct, data) do
        struct |> cast(data, [:id, :name])
      end
    end

    @primary_key false
    embedded_schema do
      field(:nextPageToken, :string, default: nil)
      embeds_many(:items, Share)
    end

    def from_data(data) do
      %Shares{}
      |> cast(data, [:nextPageToken])
      |> cast_embed(:items)
      |> apply_changes()
    end
  end

  defmodule Schemas do
    alias __MODULE__
    use Ecto.Schema
    import Ecto.Changeset

    defmodule Schema do
      alias __MODULE__
      use Ecto.Schema
      import Ecto.Changeset

      @primary_key false
      embedded_schema do
        field(:id, :string, default: nil)
        field(:name, :string)
      end

      def from_data(data) do
        %Schema{}
        |> cast(data, [:id, :name])
        |> apply_changes()
      end

      def changeset(struct, data) do
        struct |> cast(data, [:id, :name])
      end
    end

    @primary_key false
    embedded_schema do
      field(:nextPageToken, :string, default: nil)
      embeds_many(:items, Schema)
    end

    def from_data(data) do
      %Schemas{}
      |> cast(data, [:nextPageToken])
      |> cast_embed(:items)
      |> apply_changes()
    end
  end

  defmodule Tables do
    alias __MODULE__
    use Ecto.Schema
    import Ecto.Changeset

    defmodule Table do
      alias __MODULE__
      use Ecto.Schema
      import Ecto.Changeset

      @fields [:id, :name, :schema, :share, :shareId]

      @primary_key false
      embedded_schema do
        field(:id, :string, default: nil)
        field(:name, :string)
        field(:schema, :string)
        field(:share, :string)
        field(:shareId, :string, default: nil)
      end

      def from_data(data) do
        %Table{}
        |> cast(data, @fields)
        |> apply_changes()
      end

      def changeset(struct, data) do
        struct |> cast(data, @fields)
      end
    end

    @primary_key false
    embedded_schema do
      field(:nextPageToken, :string, default: nil)
      embeds_many(:items, Table)
    end

    def from_data(data) do
      %Tables{}
      |> cast(data, [:nextPageToken])
      |> cast_embed(:items)
      |> apply_changes()
    end
  end

  defmodule Protocol do
    alias __MODULE__
    use Ecto.Schema
    import Ecto.Changeset

    @primary_key false
    embedded_schema do
      field(:minReaderVersion, :integer, default: 0)
    end

    def from_data(data) do
      %Protocol{}
      |> cast(data, [:minReaderVersion])
      |> apply_changes()
    end

    def from_table_metadata_data(data) do
      from_data(Map.get(data, "protocol", %{}))
    end

    def from_table_metadata_json(json) do
      {:ok, data} = Jason.decode(json)
      from_table_metadata_data(data)
    end
  end

  defmodule MetaData do
    defmodule Table do
      alias __MODULE__
      use Ecto.Schema
      import Ecto.Changeset

      defmodule Format do
        use Ecto.Schema
        import Ecto.Changeset

        @primary_key false
        embedded_schema do
          field(:provider, :string)
        end

        def changeset(struct, data) do
          struct |> cast(data, [:provider])
        end
      end

      @primary_key false
      embedded_schema do
        field(:id, :string)
        field(:name, :string, default: nil)
        field(:description, :string, default: nil)
        embeds_one(:format, Format)
        field(:schemaString, :string)
        field(:partitionColumns, {:array, :string})
      end

      def from_data(data) do
        %Table{}
        |> cast(data, [:id, :name, :description, :schemaString, :partitionColumns])
        |> cast_embed(:format)
        |> apply_changes()
      end

      def from_table_metadata_data(data) do
        from_data(Map.get(data, "metaData", %{}))
      end

      def from_table_metadata_json(json) do
        {:ok, data} = Jason.decode(json)
        from_table_metadata_data(data)
      end
    end
  end

  defmodule TableMeta do
    alias __MODULE__
    defstruct protocol: nil, metadata: nil

    def new(protocol, metadata) do
      %TableMeta{protocol: protocol, metadata: metadata}
    end
  end

  defmodule Query do
    alias __MODULE__
    defstruct protocol: nil, metadata: nil, files: []

    def new(protocol, metadata, files) do
      %Query{protocol: protocol, metadata: metadata, files: files}
    end

    defmodule File do
      alias __MODULE__
      use Ecto.Schema
      import Ecto.Changeset

      @primary_key false
      embedded_schema do
        field(:url, :string)
        field(:id, :string)
        field(:partitionValues, :map)
        field(:size, :integer)
        field(:stats, :string, default: nil)
      end

      def from_data(data) do
        %File{}
        |> cast(data, [:url, :id, :partitionValues, :size, :stats])
        |> apply_changes()
      end

      def from_query_table_data(data) do
        from_data(Map.get(data, "file", %{}))
      end

      def from_query_table_json(json) do
        {:ok, data} = Jason.decode(json)
        from_query_table_data(data)
      end
    end
  end
end
