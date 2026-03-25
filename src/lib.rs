// src/lib.rs

//! Mi Librería P2P
//! 
//! Una librería robusta para construir nodos P2P y Relays utilizando libp2p.

// Módulos públicos: el usuario final necesita acceder a esto para configurar e interactuar con el nodo.
pub mod builder;
pub mod client;
pub mod events;

// Módulos internos: contienen la lógica de red pesada, ocultos al usuario final.
pub(crate) mod behaviour;
pub(crate) mod event_loop;

// Re-exportamos las estructuras principales en la raíz de la librería
// para que el usuario pueda usar `mi_libreria_p2p::P2pClient` directamente.
pub use builder::P2pNodeBuilder;
pub use client::P2pClient;
pub use events::NetworkEvent;