use polars::prelude::*;

fn main() -> Result<()> {
    let mut state_fin = CsvReader::from_path("examples/merge/data/state_gov_finances.csv")?
            .has_header(true)
            .finish()?;
    let state_shapes = CsvReader::from_path("examples/merge/data/tl_2021_us_state.csv")?
            .has_header(true)
            .finish()?;
    
    
    println!("{:?}", state_shapes.head(None));
    
    state_fin = state_fin.filter(&state_fin.column("YEAR")?.equal("2020"))?;
    state_fin = state_fin.filter(&state_fin.column("GOVTYPE")?.equal("002"))?;
    state_fin = state_fin.filter(&state_fin.column("AGG_DESC")?.equal("SF0001"))?;

    state_fin.apply("GEO_ID", |geoids|{
        geoids.utf8()
            .unwrap()
            .into_iter()
            .map(|opt_name| {
                opt_name.map(|name| 
                    name[(name.len() - 2)..].parse::<i64>().unwrap()
                )
            })
            .collect::<Int64Chunked>()
            .into_series()
    })?;
    
    println!("{:?}", state_fin.head(None));
    
    println!("{:?}\n{:?}", state_shapes.dtypes(), state_fin.dtypes());

    let states_with_revenue = 
        state_shapes
        .inner_join(&state_fin, ["GEOID"], ["GEO_ID"])?
        .select(["geometry", "GEOID", "NAME", "AMOUNT"])?;

    println!("{:?}", states_with_revenue.head(Some(5)));

    Ok(())
}
