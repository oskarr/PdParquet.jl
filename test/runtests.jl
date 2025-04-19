using PandasParquet
using CategoricalArrays, Dates, TimeZones
using Test

const Optional{T} = Union{Missing, T}

@testset "PandasParquet.jl" begin
  df = PandasParquet.read_pandas_parquet("./generated/df1.parquet")
  @test eltype(df[!, "StringCol"]) <: AbstractString
  @test eltype(df[!, "CategoricalCol"]) <: Optional{CategoricalValue}
  @test eltype(df[!, "DateTimeCol"]) <: Optional{DateTime}
  @test eltype(df[!, "DateTimeColTz"]) <: Optional{ZonedDateTime}
  @test eltype(df[!, "DateTimeColUTC"]) <: Optional{ZonedDateTime}
end
