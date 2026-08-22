use common_game::components::resource::GenericResource;
use common_game::components::resource::ResourceType::{self, Complex};
use common_game::components::resource::*;
use common_game::components::resource::{BasicResource::*, ComplexResource::*};

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
    pub fn contains(&self, resource_type: ResourceType) -> usize {
        self.resources
            .iter()
            .filter(|resource| match (resource, resource_type) {
                (
                    GenericResource::BasicResources(basic_resource),
                    ResourceType::Basic(basic_resource_type),
                ) => match (basic_resource, basic_resource_type) {
                    (Oxygen(_), BasicResourceType::Oxygen) => true,
                    (Hydrogen(_), BasicResourceType::Hydrogen) => true,
                    (Carbon(_), BasicResourceType::Carbon) => true,
                    (Silicon(_), BasicResourceType::Silicon) => true,
                    _ => false,
                },
                (
                    GenericResource::ComplexResources(complex_resource),
                    Complex(complex_resource_type),
                ) => match (complex_resource, complex_resource_type) {
                    (Diamond(_), ComplexResourceType::Diamond) => true,
                    (Water(_), ComplexResourceType::Water) => true,
                    (Life(_), ComplexResourceType::Life) => true,
                    (Robot(_), ComplexResourceType::Robot) => true,
                    (Dolphin(_), ComplexResourceType::Dolphin) => true,
                    (AIPartner(_), ComplexResourceType::AIPartner) => true,
                    _ => false,
                },
                _ => false,
            })
            .count()
    }
}

pub trait Explorer {
    fn run(&mut self);
}

#[cfg(test)]
mod tests {
    use common_game::{
        components::{
            energy_cell::EnergyCell, resource::GenericResource::BasicResources, sunray::Sunray,
        },
        protocols::{
            orchestrator_planet::{OrchestratorToPlanet, PlanetToOrchestrator},
            planet_explorer::ExplorerToPlanet,
        },
    };

    use super::*;

    fn get_charged_energy_cell() -> EnergyCell {
        let mut energy_cell = EnergyCell::default();
        energy_cell.charge(Sunray::default());
        energy_cell
    }

    fn make_bag() -> Bag {
        let mut vec: Vec<GenericResource> = Vec::new();

        let (_, rx) = crossbeam_channel::unbounded::<OrchestratorToPlanet>();
        let (tx, _) = crossbeam_channel::unbounded::<PlanetToOrchestrator>();
        let (_, rx_explorer) = crossbeam_channel::unbounded::<ExplorerToPlanet>();

        let planet = planet::create_planet(0, rx, tx, rx_explorer);

        // Adds water
        if let Ok(oxygen) = planet
            .generator()
            .make_oxygen(&mut get_charged_energy_cell())
        {
            if let Ok(hydrogen) = planet
                .generator()
                .make_hydrogen(&mut get_charged_energy_cell())
            {
                if let Ok(comp_res) =
                    planet
                        .combinator()
                        .make_water(hydrogen, oxygen, &mut get_charged_energy_cell())
                {
                    vec.push(GenericResource::ComplexResources(Water(comp_res)));
                }
            }
        }

        // Adds carbon
        if let Ok(carbon) = planet
            .generator()
            .make_carbon(&mut get_charged_energy_cell())
        {
            vec.push(BasicResources(Carbon(carbon)));
        }
        // Adds silicon
        if let Ok(silicon) = planet
            .generator()
            .make_silicon(&mut get_charged_energy_cell())
        {
            vec.push(BasicResources(Silicon(silicon)));
        }

        Bag { resources: vec }
    }

    #[test]
    fn test_contains() {
        let bag = make_bag();

        assert_eq!(
            bag.contains(ResourceType::Basic(BasicResourceType::Oxygen)),
            0
        );
        assert_eq!(
            bag.contains(ResourceType::Basic(BasicResourceType::Hydrogen)),
            0
        );
        assert_eq!(
            bag.contains(ResourceType::Basic(BasicResourceType::Carbon)),
            1
        );
        assert_eq!(
            bag.contains(ResourceType::Basic(BasicResourceType::Silicon)),
            1
        );
        assert_eq!(
            bag.contains(ResourceType::Complex(ComplexResourceType::Water)),
            1
        );
        assert_eq!(
            bag.contains(ResourceType::Complex(ComplexResourceType::Diamond)),
            0
        );
        assert_eq!(
            bag.contains(ResourceType::Complex(ComplexResourceType::Life)),
            0
        );
        assert_eq!(
            bag.contains(ResourceType::Complex(ComplexResourceType::Robot)),
            0
        );
        assert_eq!(
            bag.contains(ResourceType::Complex(ComplexResourceType::Dolphin)),
            0
        );
        assert_eq!(
            bag.contains(ResourceType::Complex(ComplexResourceType::AIPartner)),
            0
        );
    }
}
