use libp2p::identity::Keypair;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use crate::error::P2pError;

/// Carga un Keypair desde el disco o genera uno nuevo si no existe.
///
/// La clave se serializa usando el formato protobuf de `libp2p`.
pub fn load_or_generate(path: &Path) -> Result<Keypair, P2pError> {
    if path.exists() {
        // Leer la clave existente
        let mut file = File::open(path)?;
        let mut encoded = Vec::new();
        file.read_to_end(&mut encoded)?;
        
        // Decodificar desde el formato protobuf estándar de libp2p
        let keypair = Keypair::from_protobuf_encoding(&encoded)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Clave corrupta o formato inválido"))?;
            
        tracing::info!("Identidad cargada desde disco. PeerId: {}", keypair.public().to_peer_id());
        Ok(keypair)
    } else {
        // Generar nueva clave Ed25519
        tracing::info!("No se encontró identidad en {:?}. Generando una nueva...", path);
        let keypair = Keypair::generate_ed25519();
        
        // Guardar en disco en formato protobuf
        let encoded = keypair.to_protobuf_encoding()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Error al codificar la clave"))?;
            
        // Asegurarnos de crear el directorio si no existe
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        let mut file = File::create(path)?;
        file.write_all(&encoded)?;
        
        tracing::info!("Nueva identidad generada y guardada. PeerId: {}", keypair.public().to_peer_id());
        Ok(keypair)
    }
}