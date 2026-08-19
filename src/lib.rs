use common_game::components::resource::GenericResource;

pub mod logged_channel;

#[derive(Debug)]
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

pub trait Explorer {
    fn run(&mut self);
}
