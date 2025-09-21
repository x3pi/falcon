fn main() {
    println!("cargo:warning=Running consensus/build.rs");

    let out_dir = std::env::var("OUT_DIR").unwrap();
    println!("cargo:warning=OUT_DIR: {}", out_dir);

    let proto_file = "proto/executor.proto";
    let proto_dir = "proto/";

    println!("cargo:warning=Compiling proto file: {} from directory: {}", proto_file, proto_dir);

    let mut config = prost_build::Config::new();
    config.out_dir(&out_dir); // Đảm bảo đầu ra vào OUT_DIR
    config.compile_protos(&[proto_file], &[proto_dir])
        .map_err(|e| {
            println!("cargo:warning=Failed to compile protos: {:?}", e);
            e
        })
        .unwrap();

    println!("cargo:warning=Successfully compiled protos.");
}