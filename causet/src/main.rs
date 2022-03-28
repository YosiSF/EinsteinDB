mod mutant_searches;
mod mutant_search;
mod range;


fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut searcher = mutant_searches::MutantSearcher::new();
    searcher.search(&args[1]);
}


