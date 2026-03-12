#[path = ""]
pub mod common {
    #[path = "common.v2.rs"]
    pub mod v2;
}

#[path = ""]
pub mod errordetails {
    #[path = "errordetails.v2.rs"]
    pub mod v2;
}

#[path = ""]
pub mod dfdaemon {
    #[path = "dfdaemon.v2.rs"]
    pub mod v2;
}

#[path = ""]
pub mod manager {
    #[path = "manager.v2.rs"]
    pub mod v2;
}

#[path = ""]
pub mod scheduler {
    #[path = "scheduler.v2.rs"]
    pub mod v2;
}

// FILE_DESCRIPTOR_SET is the serialized FileDescriptorSet of the proto files.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("descriptor.bin");
