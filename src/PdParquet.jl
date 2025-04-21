module PdParquet
    export read_pandas_parquet
    
    using Parquet, DataFrames, JSON3, CategoricalArrays
    using Dates, TimeZones

    # See https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-units for mapping
    const NUMPY_DATETIME_UNITS = Dict{String, Period}(
        "D" => Dates.Day(1),
        "W" => Dates.Week(1),
        "M" => Dates.Month(1),
        "Y" => Dates.Year(1),
        "h" => Dates.Hour(1),
        "m" => Dates.Minute(1),
        "s" => Dates.Second(1),
        "ms" => Dates.Millisecond(1),
        "us" => Dates.Microsecond(1),
        "ns" => Dates.Nanosecond(1),
        # Julialang's Dates package does not allow sub-nanosecond precision
        # "ps" => Dates.Picosecond(1),
        # "fs" => Dates.Femtosecond(1),
        # "as" => Dates.Attosecond(1)
    );

    const EPOCH = Dates.DateTime(1970, 1, 1)

    ## Parsers
    function parse_timedelta(data::AbstractVector, resolution::T)::Vector{<:Union{T, Missing}} where T <: TimePeriod
        local out::Vector{Union{Missing, T}}
        # Perhaps on-the-fly masking "could" be faster (but LLVM is smart)
        local mask = @. !ismissing(data)
        out = zeros(length(data)) .* missing
        out[mask] = resolution * data[mask]
        if !any(mask)
            return out::Vector{T}
        else
            return out
        end
    end
    
    function parse_datetime(data::AbstractVector, resolution::TimePeriod)::Vector{<:Union{Missing, DateTime}}
        global EPOCH
        local out::Vector{Union{Missing, DateTime}}
        local mask = @. !ismissing(data)
        out = zeros(length(data)) .* missing
        out[mask] = DateTime.(EPOCH + resolution * data[mask])
        if !any(mask)
            return out::Vector{DateTime}
        else
            return out
        end
    end
    
    function parse_datetimetz(data::AbstractVector, tz::TimeZone, resolution::TimePeriod)::Vector{<:Union{Missing, ZonedDateTime}}
        global EPOCH
        local out::Vector{Union{Missing, ZonedDateTime}}
        local mask = @. !ismissing(data)
        out = zeros(length(data)) .* missing
        out[mask] = ZonedDateTime.(EPOCH + resolution * data[mask], tz, from_utc=true)
        if !any(mask)
            return out::Vector{ZonedDateTime}
        else
            return out
        end
    end
    
    function parse_temporal_resolution(col)::TimePeriod
        np_res_re = r"^(?:datetime64|timedelta64)\[(.*)\]$"
        keys = match(np_res_re, col["numpy_type"]).captures
        @assert length(keys) == 1
        return NUMPY_DATETIME_UNITS[keys[1]]
    end

    ## Main
    function read_pandas_parquet(filepath::String)::DataFrame
        # Step 1: Read the Parquet file and metadata
        parquetfile = Parquet.File(filepath)
        metadata = Parquet.metadata(parquetfile)
    
        # Step 2: Extract and parse the pandas-specific metadata
        pandas_metadata = nothing
        for kv in metadata.key_value_metadata
            if kv.key == "pandas"
                pandas_metadata = JSON3.read(kv.value)
                break
            end
        end
    
        if isnothing(pandas_metadata)
            error("Pandas metadata not found in the Parquet file.")
        end
    
        # Step 3: Reconstruct the DataFrame
        # Extract column metadata
        columns_meta = pandas_metadata["columns"]
        # index_columns = pandas_metadata["index_columns"]
    
        # Create a dictionary to hold the DataFrame columns
        df_dict = Dict{Symbol, Vector}()
    
        raw_data = collect(BatchedColumnsCursor(parquetfile::Parquet.File))
    
        # Read the columns from the Parquet file
        for col in columns_meta
            col_name = col["name"]
            field_name = col["field_name"]
            pandas_type = col["pandas_type"]
            numpy_type = col["numpy_type"]
            metadata = get(col, "metadata", nothing)
    
            # Read the column data
            col_data = vcat([getproperty(d, Symbol(field_name)) for d in raw_data]...)
    
            # Convert the column data based on the pandas_type and numpy_type
            if pandas_type == "unicode"
                col_data = col_data .|> d -> ismissing(d) ? missing : String(d)
            elseif pandas_type == "bytes"
                col_data = col_data .|> d -> ismissing(d) ? missing : String(d)
            elseif pandas_type == "categorical"
                col_data = CategoricalArray(col_data)
            elseif pandas_type == "datetime"
                res = parse_temporal_resolution(col)
                col_data = parse_datetime(col_data, res)
            elseif pandas_type == "datetimetz"
                res = parse_temporal_resolution(col)
                timezone = TimeZone(metadata["timezone"], TimeZones.Class(:LEGACY) | TimeZones.Class(:FIXED) | TimeZones.Class(:STANDARD))
                col_data = parse_datetimetz(col_data, timezone, res)
            elseif pandas_type == "timedelta" || startswith(numpy_type, "timedelta64")
                res = parse_temporal_resolution(col)
                col_data = parse_timedelta(col_data, res)
            elseif pandas_type == "object"
                encoding = metadata["encoding"]
                if encoding == "pickle"
                    col_data = deserialize.(col_data)
                elseif encoding == "bson"
                    col_data = bsondeserialize.(col_data)
                elseif encoding == "json"
                    col_data = JSON3.read.(String.(col_data))
                end
            end
    
            # Add the column to the dictionary
            if col_name === nothing
                col_name = field_name
            end
            df_dict[Symbol(col_name)] = col_data
        end
    
        # Create the DataFrame
        df = DataFrame(df_dict)

        return df
    end
end
