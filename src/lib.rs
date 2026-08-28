use common_game::components::resource::GenericResource::{self, ComplexResources};
use common_game::components::resource::ResourceType::{self, Complex};
use common_game::components::resource::{BasicResource::*, ComplexResource::*};
use common_game::protocols::orchestrator_explorer::ExplorerToOrchestrator::{
    CurrentPlanetResult, MovedToPlanetResult,
};
use common_game::{
    components::resource::{
        BasicResourceType, ComplexResourceRequest, ComplexResourceType, GenericResource::*,
    },
    protocols::{
        orchestrator_explorer::{ExplorerToOrchestrator::*, OrchestratorToExplorer::*},
        planet_explorer::PlanetToExplorer::*,
    },
    utils::ID,
};

use common_game::protocols::orchestrator_explorer::{
    ExplorerToOrchestrator, OrchestratorToExplorer,
};
use common_game::protocols::planet_explorer::PlanetToExplorer::SupportedCombinationResponse;
use common_game::protocols::planet_explorer::{ExplorerToPlanet, PlanetToExplorer};
use crossbeam_channel::*;

use crate::logged_channel::LoggedChannel;

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
    pub fn add_resource(&mut self, resource: GenericResource) {
        self.resources.push(resource);
    }
    pub fn take_resource(&mut self, resource_type: ResourceType) -> Result<GenericResource, ()> {
        if let Some(index) = self
            .resources
            .iter()
            .position(|resource| resource.get_type() == resource_type)
        {
            return Ok(self.resources.remove(index));
        }
        Err(())
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
pub enum AiReturn {
    Stop,
    Reset,
    Kill,
}
pub trait Explorer {
    fn new(
        id: ID,
        bag: Bag,
        planet_id: ID,
        planet_channel: LoggedChannel<ExplorerToPlanet, PlanetToExplorer>,
        orchestrator_channel: LoggedChannel<
            ExplorerToOrchestrator<BagContent>,
            OrchestratorToExplorer,
        >,
    ) -> Self;
    fn explorer_ai(&mut self) -> AiReturn; // false if the explorer is dead, true otherwise
    fn run(&mut self) {
        self.set_auto_mode(false);
        loop {
            if self.get_auto_mode() {
                match self.explorer_ai() {
                    AiReturn::Stop => self.set_auto_mode(false),
                    AiReturn::Reset => {
                        self.set_auto_mode(false);
                        self.get_bag().resources.clear();
                        self.reset();
                    }
                    AiReturn::Kill => break,
                }
            }

            // Checks for a message from the orchestrator (old try_recv_from_orchestrator_and_respond())
            if let Ok(message) = self.get_orchestrator_channel().recv() {
                match message {
                    StartExplorerAI => {
                        self.set_auto_mode(true);
                        if let Ok(()) = self.get_orchestrator_channel().send(
                            ExplorerToOrchestrator::StartExplorerAIResult {
                                explorer_id: self.get_id(),
                            },
                        ) {}
                    }
                    ResetExplorerAI => {
                        self.set_auto_mode(false);
                        self.get_bag().resources.clear();
                        self.reset();
                    }
                    KillExplorer => break,
                    StopExplorerAI => self.set_auto_mode(false),
                    MoveToPlanet {
                        sender_to_new_planet,
                        planet_id,
                    } => {
                        self.set_planet_id(planet_id);
                        if let Some(new_sender) = sender_to_new_planet {
                            self.set_planet_channel_tx(new_sender);
                            if let Ok(()) =
                                self.get_orchestrator_channel().send(MovedToPlanetResult {
                                    explorer_id: self.get_id(),
                                    planet_id,
                                })
                            {}
                        }
                    }
                    CurrentPlanetRequest => {
                        if let Ok(()) = self.get_orchestrator_channel().send(CurrentPlanetResult {
                            explorer_id: self.get_id(),
                            planet_id: self.get_planet_id(),
                        }) {}
                    }
                    SupportedResourceRequest => {
                        if let Ok(()) = self.get_planet_channel().send(
                            ExplorerToPlanet::SupportedResourceRequest {
                                explorer_id: self.get_id(),
                            },
                        ) {
                            if let Ok(msg) = self.get_planet_channel().recv() {
                                if let SupportedResourceResponse { resource_list } = msg {
                                    if let Ok(()) = self.get_orchestrator_channel().send(
                                        SupportedResourceResult {
                                            explorer_id: self.get_id(),
                                            supported_resources: resource_list,
                                        },
                                    ) {}
                                }
                            }
                        }
                    }
                    SupportedCombinationRequest => {
                        if let Ok(()) = self.get_planet_channel().send(
                            ExplorerToPlanet::SupportedCombinationRequest {
                                explorer_id: self.get_id(),
                            },
                        ) {
                            if let Ok(msg) = self.get_planet_channel().recv() {
                                if let SupportedCombinationResponse { combination_list } = msg {
                                    if let Ok(()) = self.get_orchestrator_channel().send(
                                        SupportedCombinationResult {
                                            explorer_id: self.get_id(),
                                            combination_list,
                                        },
                                    ) {}
                                }
                            }
                        }
                    }
                    OrchestratorToExplorer::GenerateResourceRequest { to_generate } => {
                        if let Ok(()) = self.get_planet_channel().send(
                            ExplorerToPlanet::GenerateResourceRequest {
                                explorer_id: self.get_id(),
                                resource: to_generate,
                            },
                        ) {
                            if let Ok(msg) = self.get_planet_channel().recv() {
                                if let PlanetToExplorer::GenerateResourceResponse { resource } = msg
                                {
                                    if let Some(resource) = resource {
                                        if let Ok(()) = self.get_orchestrator_channel().send(
                                            ExplorerToOrchestrator::GenerateResourceResponse {
                                                explorer_id: self.get_id(),
                                                generated: Ok(()),
                                            },
                                        ) {
                                            self.get_bag().resources.push(BasicResources(resource));
                                        }
                                    } else {
                                        if let Ok(()) = self.get_orchestrator_channel().send(
                                            ExplorerToOrchestrator::GenerateResourceResponse {
                                                explorer_id: self.get_id(),
                                                generated: Err(String::from(
                                                    "No resource was created",
                                                )),
                                            },
                                        ) {}
                                    }
                                }
                            }
                        }
                    }
                    OrchestratorToExplorer::CombineResourceRequest { to_generate } => {
                        match to_generate {
                            ComplexResourceType::Diamond => {
                                if let (
                                    Ok(BasicResources(Carbon(res1))),
                                    Ok(BasicResources(Carbon(res2))),
                                ) = (
                                    self.get_bag().take_resource(ResourceType::Basic(
                                        BasicResourceType::Carbon,
                                    )),
                                    self.get_bag().take_resource(ResourceType::Basic(
                                        BasicResourceType::Carbon,
                                    )),
                                ) {
                                    self.combine_and_respond(ComplexResourceRequest::Diamond(
                                        res1, res2,
                                    ));
                                } else {
                                    if let Ok(()) = self.get_orchestrator_channel().send(
                                        ExplorerToOrchestrator::CombineResourceResponse {
                                            explorer_id: self.get_id(),
                                            generated: Err(String::from(
                                                "Explorer is missing the required resources",
                                            )),
                                        },
                                    ) {}
                                }
                            }
                            ComplexResourceType::Water => {
                                if let (
                                    Ok(BasicResources(Hydrogen(res1))),
                                    Ok(BasicResources(Oxygen(res2))),
                                ) = (
                                    self.get_bag().take_resource(ResourceType::Basic(
                                        BasicResourceType::Hydrogen,
                                    )),
                                    self.get_bag().take_resource(ResourceType::Basic(
                                        BasicResourceType::Oxygen,
                                    )),
                                ) {
                                    self.combine_and_respond(ComplexResourceRequest::Water(
                                        res1, res2,
                                    ));
                                } else {
                                    if let Ok(()) = self.get_orchestrator_channel().send(
                                        ExplorerToOrchestrator::CombineResourceResponse {
                                            explorer_id: self.get_id(),
                                            generated: Err(String::from(
                                                "Explorer is missing the required resources",
                                            )),
                                        },
                                    ) {}
                                }
                            }
                            ComplexResourceType::Life => {
                                if let (
                                    Ok(ComplexResources(Water(res1))),
                                    Ok(BasicResources(Carbon(res2))),
                                ) = (
                                    self.get_bag().take_resource(ResourceType::Complex(
                                        ComplexResourceType::Water,
                                    )),
                                    self.get_bag().take_resource(ResourceType::Basic(
                                        BasicResourceType::Carbon,
                                    )),
                                ) {
                                    self.combine_and_respond(ComplexResourceRequest::Life(
                                        res1, res2,
                                    ));
                                } else {
                                    if let Ok(()) = self.get_orchestrator_channel().send(
                                        ExplorerToOrchestrator::CombineResourceResponse {
                                            explorer_id: self.get_id(),
                                            generated: Err(String::from(
                                                "Explorer is missing the required resources",
                                            )),
                                        },
                                    ) {}
                                }
                            }
                            ComplexResourceType::Robot => {
                                if let (
                                    Ok(BasicResources(Silicon(res1))),
                                    Ok(ComplexResources(Life(res2))),
                                ) = (
                                    self.get_bag().take_resource(ResourceType::Basic(
                                        BasicResourceType::Silicon,
                                    )),
                                    self.get_bag().take_resource(ResourceType::Complex(
                                        ComplexResourceType::Life,
                                    )),
                                ) {
                                    self.combine_and_respond(ComplexResourceRequest::Robot(
                                        res1, res2,
                                    ));
                                } else {
                                    if let Ok(()) = self.get_orchestrator_channel().send(
                                        ExplorerToOrchestrator::CombineResourceResponse {
                                            explorer_id: self.get_id(),
                                            generated: Err(String::from(
                                                "Explorer is missing the required resources",
                                            )),
                                        },
                                    ) {}
                                }
                            }
                            ComplexResourceType::Dolphin => {
                                if let (
                                    Ok(ComplexResources(Water(res1))),
                                    Ok(ComplexResources(Life(res2))),
                                ) = (
                                    self.get_bag().take_resource(ResourceType::Complex(
                                        ComplexResourceType::Water,
                                    )),
                                    self.get_bag().take_resource(ResourceType::Complex(
                                        ComplexResourceType::Life,
                                    )),
                                ) {
                                    self.combine_and_respond(ComplexResourceRequest::Dolphin(
                                        res1, res2,
                                    ));
                                } else {
                                    if let Ok(()) = self.get_orchestrator_channel().send(
                                        ExplorerToOrchestrator::CombineResourceResponse {
                                            explorer_id: self.get_id(),
                                            generated: Err(String::from(
                                                "Explorer is missing the required resources",
                                            )),
                                        },
                                    ) {}
                                }
                            }
                            ComplexResourceType::AIPartner => {
                                if let (
                                    Ok(ComplexResources(Robot(res1))),
                                    Ok(ComplexResources(Diamond(res2))),
                                ) = (
                                    self.get_bag().take_resource(ResourceType::Complex(
                                        ComplexResourceType::Robot,
                                    )),
                                    self.get_bag().take_resource(ResourceType::Complex(
                                        ComplexResourceType::Diamond,
                                    )),
                                ) {
                                    self.combine_and_respond(ComplexResourceRequest::AIPartner(
                                        res1, res2,
                                    ));
                                } else {
                                    if let Ok(()) = self.get_orchestrator_channel().send(
                                        ExplorerToOrchestrator::CombineResourceResponse {
                                            explorer_id: self.get_id(),
                                            generated: Err(String::from(
                                                "Explorer is missing the required resources",
                                            )),
                                        },
                                    ) {}
                                }
                            }
                        }
                    }
                    BagContentRequest => {
                        if let Ok(()) = self.get_orchestrator_channel().send(BagContentResponse {
                            explorer_id: self.get_id(),
                            bag_content: BagContent::from(self.get_bag()),
                        }) {}
                    }
                    NeighborsResponse { neighbors } => {
                        // Never going to happen here as when it happens is in response
                        // to the explorer request, after which the explorer will block
                        // and wait for this response
                    }
                }
            } else {
                break;
            }
        }
    }
    fn reset(&mut self);
    // Getter and setters to force the use of these attributes in any Explorer trait implementation
    // So that the explorer response logic to orchestrator messages can be shared by explorers
    fn get_id(&self) -> ID;
    fn get_bag(&mut self) -> &mut Bag;
    fn get_planet_id(&self) -> ID;
    fn set_planet_id(&mut self, new: ID);
    fn get_auto_mode(&self) -> bool;
    fn set_auto_mode(&mut self, mode: bool);
    fn get_planet_channel(&self) -> LoggedChannel<ExplorerToPlanet, PlanetToExplorer>;
    fn set_planet_channel_tx(&mut self, tx: Sender<ExplorerToPlanet>);
    fn set_planet_channel_rx(&mut self, rx: Receiver<PlanetToExplorer>);
    fn get_orchestrator_channel(
        &self,
    ) -> LoggedChannel<ExplorerToOrchestrator<BagContent>, OrchestratorToExplorer>;
    fn set_orchestrator_channel_tx(&mut self, tx: Sender<ExplorerToOrchestrator<BagContent>>);
    fn set_orchestrator_channel_rx(&mut self, rx: Receiver<OrchestratorToExplorer>);

    //
    fn is_combination_available(&self, resource: ComplexResourceType) -> bool {
        if let Ok(()) =
            self.get_planet_channel()
                .send(ExplorerToPlanet::SupportedCombinationRequest {
                    explorer_id: self.get_id(),
                })
        {
            if let Ok(msg) = self.get_planet_channel().recv() {
                if let SupportedCombinationResponse { combination_list } = msg {
                    return combination_list.contains(&resource);
                }
            }
        }
        false
    }
    fn combine_and_respond(&mut self, complex_resource_request: ComplexResourceRequest) {
        if let Ok(()) = self
            .get_planet_channel()
            .send(ExplorerToPlanet::CombineResourceRequest {
                explorer_id: self.get_id(),
                msg: complex_resource_request,
            })
        {
            if let Ok(response) = self.get_planet_channel().recv() {
                if let PlanetToExplorer::CombineResourceResponse { complex_response } = response {
                    if let Ok(()) = self.get_orchestrator_channel().send(
                        ExplorerToOrchestrator::CombineResourceResponse {
                            explorer_id: self.get_id(),
                            generated: match complex_response {
                                Ok(complex_resource) => {
                                    self.get_bag()
                                        .add_resource(ComplexResources(complex_resource));
                                    Ok(())
                                }
                                Err((err, res1, res2)) => {
                                    self.get_bag().add_resource(res1);
                                    self.get_bag().add_resource(res2);
                                    Err(err)
                                }
                            },
                        },
                    ) {}
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct BagContent {
    pub resources: Vec<ResourceType>,
}

impl From<&mut Bag> for BagContent {
    fn from(bag: &mut Bag) -> Self {
        let mut vec = Vec::new();
        for i in &bag.resources {
            vec.push(i.get_type());
        }
        Self { resources: vec }
    }
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
    #[test]
    fn test_take_resource() {
        let mut bag = make_bag();

        let taken = bag.take_resource(ResourceType::Complex(ComplexResourceType::Water));

        assert_eq!(
            taken.unwrap().get_type(),
            ResourceType::Complex(ComplexResourceType::Water)
        );
    }
}
