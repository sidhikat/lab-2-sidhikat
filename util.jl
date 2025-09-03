using DataFrames
using Dates
using Unitful
using CSV
using TidierFiles

"""
Parse station header with 6 parts: ID, NAME, STATE, LAT, LON, ELEVATION
"""
function parse_station_parts(station_number::Int, id, name, state, lat_str, lon_str, elevation, years_of_data::Int=0)
    return (
        stnid = station_number,
        noaa_id = String(strip(id)),
        name = String(strip(name)),
        state = String(strip(state)),
        latitude = parse(Float64, strip(lat_str)),
        longitude = parse(Float64, strip(lon_str)),
        years_of_data = years_of_data
    )
end

"""
Parse station header with 7 parts: ID, NAME, CITY, STATE, LAT, LON, ELEVATION
"""
function parse_station_parts(station_number::Int, id, name, city, state, lat_str, lon_str, elevation, years_of_data::Int=0)
    # Combine name and city for the full name
    full_name = "$(strip(name)), $(strip(city))"
    return (
        stnid = station_number,
        noaa_id = String(strip(id)),
        name = String(full_name),
        state = String(strip(state)),
        latitude = parse(Float64, strip(lat_str)),
        longitude = parse(Float64, strip(lon_str)),
        years_of_data = years_of_data
    )
end

"""
    parse_station_header(header_line::String, station_number::Int, years_of_data::Int)

Parse a station header line using method overloading for different formats.
"""
function parse_station_header(header_line::AbstractString, station_number::Int, years_of_data::Int=0)
    # Split by commas and strip whitespace
    parts = [strip(p) for p in split(header_line, ",")]
    
    # Use splatting with method overloading
    return parse_station_parts(station_number, parts..., years_of_data)
end

"""
    parse_rainfall_data(data_lines::Vector{String}, rainfall_unit, stnid::Int)

Parse rainfall data lines and return DataFrame with dates, years, rainfall, and station ID.
Fills missing years with missing values to create complete time series.
"""
function parse_rainfall_data(data_lines::Vector{<:AbstractString}, rainfall_unit, stnid::Int)
    dates = Date[]
    years = Int[]
    rainfall = Float64[]
    stnids = Int[]

    # Parse available data
    for line in data_lines
        line = strip(line)
        if !isempty(line)
            parts = split(line)
            if length(parts) >= 2
                date_str = parts[1]
                rain_str = parts[2]

                date = Date(date_str, "mm/dd/yyyy")
                rain_val = parse(Float64, rain_str)

                push!(dates, date)
                push!(years, year(date))
                push!(rainfall, rain_val)
                push!(stnids, stnid)
            end
        end
    end

    # If no data, return empty DataFrame
    if isempty(years)
        return DataFrame(
            stnid = Int[],
            date = Date[],
            year = Int[],
            rainfall = typeof(1.0 * rainfall_unit)[]
        )
    end

    # Create complete year sequence and fill missing years
    min_year, max_year = extrema(years)
    complete_years = collect(min_year:max_year)
    
    # Create vectors for complete time series
    complete_stnids = Int[]
    complete_dates = Date[]
    complete_years_vec = Int[]
    complete_rainfall = Union{Float64, Missing}[]
    
    for yr in complete_years
        push!(complete_stnids, stnid)
        push!(complete_years_vec, yr)
        
        # Find if this year has data
        year_idx = findfirst(==(yr), years)
        if year_idx !== nothing
            push!(complete_dates, dates[year_idx])
            push!(complete_rainfall, rainfall[year_idx])
        else
            # Missing year - use January 1st as placeholder date
            push!(complete_dates, Date(yr, 1, 1))
            push!(complete_rainfall, missing)
        end
    end

    return DataFrame(
        stnid = complete_stnids,
        date = complete_dates,
        year = complete_years_vec,
        rainfall = complete_rainfall .* rainfall_unit
    )
end

"""
    read_noaa_data(filename::String)

Parse NOAA precipitation data file and return structured data.

Returns:
- stations: DataFrame with station metadata (stnid, noaa_id, name, state, latitude, longitude, years_of_data)
- rainfall_data: DataFrame with rainfall data (stnid, date, year, rainfall)

The rainfall DataFrame contains all rainfall data with stnid as a column to link to stations.
"""
function read_noaa_data(filename::String)
    # Read the data file
    txt = read(filename, String)
    lines = split(txt, '\n')

    # (1) Extract header info and set units
    header = lines[1]
    rainfall_unit = u"inch"

    remaining_content = join(lines[2:end], '\n')

    # (2) Split by blank lines which separate each gauge
    station_blocks = filter(!isempty, split(remaining_content, r"\n\s*\n"))

    stations = []
    all_rainfall_data = DataFrame[]

    # (3) For each gauge
    for (i, block) in enumerate(station_blocks)
        block_lines = split(strip(block), '\n')

        if !isempty(block_lines)
            # (a) Pull out the header row into station information
            header_line = block_lines[1]
            
            # (b) Parse the rainfall data with station ID
            data_lines = block_lines[2:end]
            rainfall_df = parse_rainfall_data(data_lines, rainfall_unit, i)
            years_count = nrow(rainfall_df)
            
            # Create station with years_of_data
            station = parse_station_header(header_line, i, years_count)
            push!(stations, station)
            push!(all_rainfall_data, rainfall_df)
        end
    end

    # Convert stations vector to DataFrame
    stations_df = DataFrame(stations)
    
    # Combine all rainfall data into single DataFrame
    rainfall_data_df = vcat(all_rainfall_data...)
    
    return stations_df, rainfall_data_df
end

# Simple test function
function test_read_noaa_data()
    # Create minimal test data
    test_content = """1-d, Annual Maximum, WaterYear=1 (January - December), Units in Inches
60-0011, CLEAR CK AT BAY AREA BLVD               , TX,  29.4977,  -95.1599, 2
06/11/1987    6.31
09/02/1988    5.46

60-0019, TURKEY CK AT FM 1959                    , TX,  29.5845,  -95.1869, 28
06/11/1987    3.99
09/02/1988    3.71
"""

    # Write test file
    test_file = "test_noaa.txt"
    write(test_file, test_content)

    try
        # Test the function
        stations, rainfall_data = read_noaa_data(test_file)

        # Basic tests
        @assert length(stations) == 2 "Expected 2 stations, got $(length(stations))"
        @assert length(rainfall_data) == 2 "Expected 2 rainfall datasets, got $(length(rainfall_data))"
        @assert stations[1].stnid == "stn_1" "Expected stn_1, got $(stations[1].stnid)"
        @assert stations[1].noaa_id == "60-0011" "Expected 60-0011, got $(stations[1].noaa_id)"
        @assert nrow(rainfall_data["stn_1"]) == 2 "Expected 2 rainfall records for stn_1"

        println("✓ All tests passed!")
        return true
    catch e
        println("✗ Test failed: $e")
        return false
    finally
        # Clean up test file
        if isfile(test_file)
            rm(test_file)
        end
    end
end
