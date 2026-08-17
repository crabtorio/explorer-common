use common_game::components::resource::GenericResource;

struct Bag {
    resources: Vec<GenericResource>,
}
impl Bag {
    pub fn new() -> Self {
        Bag {
            resources: Vec::new(),
        }
    }
}
