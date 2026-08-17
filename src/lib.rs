use common_game::components::resource::GenericResource;

pub struct Bag {
    pub resources: Vec<GenericResource>,
}
impl Bag {
    pub fn new() -> Self {
        Bag {
            resources: Vec::new(),
        }
    }
}

trait Explorer {
    fn run(&mut self);
}
