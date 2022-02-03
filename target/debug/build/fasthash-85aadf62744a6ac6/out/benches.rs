
#[bench]
fn bench_city_hash32_key_16(b: &mut Bencher) {
    bench_fasthash::<city::CityHash32>(b, 16);
}
#[bench]
fn bench_city_hash32_key_32(b: &mut Bencher) {
    bench_fasthash::<city::CityHash32>(b, 32);
}
#[bench]
fn bench_city_hash32_key_64(b: &mut Bencher) {
    bench_fasthash::<city::CityHash32>(b, 64);
}
#[bench]
fn bench_city_hash32_key_128(b: &mut Bencher) {
    bench_fasthash::<city::CityHash32>(b, 128);
}
#[bench]
fn bench_city_hash32_key_256(b: &mut Bencher) {
    bench_fasthash::<city::CityHash32>(b, 256);
}
#[bench]
fn bench_city_hash32_key_512(b: &mut Bencher) {
    bench_fasthash::<city::CityHash32>(b, 512);
}
#[bench]
fn bench_city_hash64_key_16(b: &mut Bencher) {
    bench_fasthash::<city::CityHash64>(b, 16);
}
#[bench]
fn bench_city_hash64_key_32(b: &mut Bencher) {
    bench_fasthash::<city::CityHash64>(b, 32);
}
#[bench]
fn bench_city_hash64_key_64(b: &mut Bencher) {
    bench_fasthash::<city::CityHash64>(b, 64);
}
#[bench]
fn bench_city_hash64_key_128(b: &mut Bencher) {
    bench_fasthash::<city::CityHash64>(b, 128);
}
#[bench]
fn bench_city_hash64_key_256(b: &mut Bencher) {
    bench_fasthash::<city::CityHash64>(b, 256);
}
#[bench]
fn bench_city_hash64_key_512(b: &mut Bencher) {
    bench_fasthash::<city::CityHash64>(b, 512);
}
#[bench]
fn bench_city_hash128_key_16(b: &mut Bencher) {
    bench_fasthash::<city::CityHash128>(b, 16);
}
#[bench]
fn bench_city_hash128_key_32(b: &mut Bencher) {
    bench_fasthash::<city::CityHash128>(b, 32);
}
#[bench]
fn bench_city_hash128_key_64(b: &mut Bencher) {
    bench_fasthash::<city::CityHash128>(b, 64);
}
#[bench]
fn bench_city_hash128_key_128(b: &mut Bencher) {
    bench_fasthash::<city::CityHash128>(b, 128);
}
#[bench]
fn bench_city_hash128_key_256(b: &mut Bencher) {
    bench_fasthash::<city::CityHash128>(b, 256);
}
#[bench]
fn bench_city_hash128_key_512(b: &mut Bencher) {
    bench_fasthash::<city::CityHash128>(b, 512);
}
#[bench]
fn bench_city_hash_crc128_key_16(b: &mut Bencher) {
    bench_fasthash::<city::CityHashCrc128>(b, 16);
}
#[bench]
fn bench_city_hash_crc128_key_32(b: &mut Bencher) {
    bench_fasthash::<city::CityHashCrc128>(b, 32);
}
#[bench]
fn bench_city_hash_crc128_key_64(b: &mut Bencher) {
    bench_fasthash::<city::CityHashCrc128>(b, 64);
}
#[bench]
fn bench_city_hash_crc128_key_128(b: &mut Bencher) {
    bench_fasthash::<city::CityHashCrc128>(b, 128);
}
#[bench]
fn bench_city_hash_crc128_key_256(b: &mut Bencher) {
    bench_fasthash::<city::CityHashCrc128>(b, 256);
}
#[bench]
fn bench_city_hash_crc128_key_512(b: &mut Bencher) {
    bench_fasthash::<city::CityHashCrc128>(b, 512);
}
#[bench]
fn bench_farm_hash32_key_16(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash32>(b, 16);
}
#[bench]
fn bench_farm_hash32_key_32(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash32>(b, 32);
}
#[bench]
fn bench_farm_hash32_key_64(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash32>(b, 64);
}
#[bench]
fn bench_farm_hash32_key_128(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash32>(b, 128);
}
#[bench]
fn bench_farm_hash32_key_256(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash32>(b, 256);
}
#[bench]
fn bench_farm_hash32_key_512(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash32>(b, 512);
}
#[bench]
fn bench_farm_hash64_key_16(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash64>(b, 16);
}
#[bench]
fn bench_farm_hash64_key_32(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash64>(b, 32);
}
#[bench]
fn bench_farm_hash64_key_64(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash64>(b, 64);
}
#[bench]
fn bench_farm_hash64_key_128(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash64>(b, 128);
}
#[bench]
fn bench_farm_hash64_key_256(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash64>(b, 256);
}
#[bench]
fn bench_farm_hash64_key_512(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash64>(b, 512);
}
#[bench]
fn bench_farm_hash128_key_16(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash128>(b, 16);
}
#[bench]
fn bench_farm_hash128_key_32(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash128>(b, 32);
}
#[bench]
fn bench_farm_hash128_key_64(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash128>(b, 64);
}
#[bench]
fn bench_farm_hash128_key_128(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash128>(b, 128);
}
#[bench]
fn bench_farm_hash128_key_256(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash128>(b, 256);
}
#[bench]
fn bench_farm_hash128_key_512(b: &mut Bencher) {
    bench_fasthash::<farm::FarmHash128>(b, 512);
}
#[bench]
fn bench_lookup3_key_16(b: &mut Bencher) {
    bench_fasthash::<lookup3::Lookup3>(b, 16);
}
#[bench]
fn bench_lookup3_key_32(b: &mut Bencher) {
    bench_fasthash::<lookup3::Lookup3>(b, 32);
}
#[bench]
fn bench_lookup3_key_64(b: &mut Bencher) {
    bench_fasthash::<lookup3::Lookup3>(b, 64);
}
#[bench]
fn bench_lookup3_key_128(b: &mut Bencher) {
    bench_fasthash::<lookup3::Lookup3>(b, 128);
}
#[bench]
fn bench_lookup3_key_256(b: &mut Bencher) {
    bench_fasthash::<lookup3::Lookup3>(b, 256);
}
#[bench]
fn bench_lookup3_key_512(b: &mut Bencher) {
    bench_fasthash::<lookup3::Lookup3>(b, 512);
}
#[bench]
fn bench_metro_hash64_1_key_16(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_1>(b, 16);
}
#[bench]
fn bench_metro_hash64_1_key_32(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_1>(b, 32);
}
#[bench]
fn bench_metro_hash64_1_key_64(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_1>(b, 64);
}
#[bench]
fn bench_metro_hash64_1_key_128(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_1>(b, 128);
}
#[bench]
fn bench_metro_hash64_1_key_256(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_1>(b, 256);
}
#[bench]
fn bench_metro_hash64_1_key_512(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_1>(b, 512);
}
#[bench]
fn bench_metro_hash64_2_key_16(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_2>(b, 16);
}
#[bench]
fn bench_metro_hash64_2_key_32(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_2>(b, 32);
}
#[bench]
fn bench_metro_hash64_2_key_64(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_2>(b, 64);
}
#[bench]
fn bench_metro_hash64_2_key_128(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_2>(b, 128);
}
#[bench]
fn bench_metro_hash64_2_key_256(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_2>(b, 256);
}
#[bench]
fn bench_metro_hash64_2_key_512(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64_2>(b, 512);
}
#[bench]
fn bench_metro_hash128_1_key_16(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_1>(b, 16);
}
#[bench]
fn bench_metro_hash128_1_key_32(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_1>(b, 32);
}
#[bench]
fn bench_metro_hash128_1_key_64(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_1>(b, 64);
}
#[bench]
fn bench_metro_hash128_1_key_128(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_1>(b, 128);
}
#[bench]
fn bench_metro_hash128_1_key_256(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_1>(b, 256);
}
#[bench]
fn bench_metro_hash128_1_key_512(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_1>(b, 512);
}
#[bench]
fn bench_metro_hash128_2_key_16(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_2>(b, 16);
}
#[bench]
fn bench_metro_hash128_2_key_32(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_2>(b, 32);
}
#[bench]
fn bench_metro_hash128_2_key_64(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_2>(b, 64);
}
#[bench]
fn bench_metro_hash128_2_key_128(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_2>(b, 128);
}
#[bench]
fn bench_metro_hash128_2_key_256(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_2>(b, 256);
}
#[bench]
fn bench_metro_hash128_2_key_512(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128_2>(b, 512);
}
#[bench]
fn bench_metro_hash64_crc_1_key_16(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_1>(b, 16);
}
#[bench]
fn bench_metro_hash64_crc_1_key_32(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_1>(b, 32);
}
#[bench]
fn bench_metro_hash64_crc_1_key_64(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_1>(b, 64);
}
#[bench]
fn bench_metro_hash64_crc_1_key_128(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_1>(b, 128);
}
#[bench]
fn bench_metro_hash64_crc_1_key_256(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_1>(b, 256);
}
#[bench]
fn bench_metro_hash64_crc_1_key_512(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_1>(b, 512);
}
#[bench]
fn bench_metro_hash64_crc_2_key_16(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_2>(b, 16);
}
#[bench]
fn bench_metro_hash64_crc_2_key_32(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_2>(b, 32);
}
#[bench]
fn bench_metro_hash64_crc_2_key_64(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_2>(b, 64);
}
#[bench]
fn bench_metro_hash64_crc_2_key_128(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_2>(b, 128);
}
#[bench]
fn bench_metro_hash64_crc_2_key_256(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_2>(b, 256);
}
#[bench]
fn bench_metro_hash64_crc_2_key_512(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash64Crc_2>(b, 512);
}
#[bench]
fn bench_metro_hash128_crc_1_key_16(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_1>(b, 16);
}
#[bench]
fn bench_metro_hash128_crc_1_key_32(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_1>(b, 32);
}
#[bench]
fn bench_metro_hash128_crc_1_key_64(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_1>(b, 64);
}
#[bench]
fn bench_metro_hash128_crc_1_key_128(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_1>(b, 128);
}
#[bench]
fn bench_metro_hash128_crc_1_key_256(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_1>(b, 256);
}
#[bench]
fn bench_metro_hash128_crc_1_key_512(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_1>(b, 512);
}
#[bench]
fn bench_metro_hash128_crc_2_key_16(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_2>(b, 16);
}
#[bench]
fn bench_metro_hash128_crc_2_key_32(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_2>(b, 32);
}
#[bench]
fn bench_metro_hash128_crc_2_key_64(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_2>(b, 64);
}
#[bench]
fn bench_metro_hash128_crc_2_key_128(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_2>(b, 128);
}
#[bench]
fn bench_metro_hash128_crc_2_key_256(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_2>(b, 256);
}
#[bench]
fn bench_metro_hash128_crc_2_key_512(b: &mut Bencher) {
    bench_fasthash::<metro::MetroHash128Crc_2>(b, 512);
}
#[bench]
fn bench_mum_hash_key_16(b: &mut Bencher) {
    bench_fasthash::<mum::MumHash>(b, 16);
}
#[bench]
fn bench_mum_hash_key_32(b: &mut Bencher) {
    bench_fasthash::<mum::MumHash>(b, 32);
}
#[bench]
fn bench_mum_hash_key_64(b: &mut Bencher) {
    bench_fasthash::<mum::MumHash>(b, 64);
}
#[bench]
fn bench_mum_hash_key_128(b: &mut Bencher) {
    bench_fasthash::<mum::MumHash>(b, 128);
}
#[bench]
fn bench_mum_hash_key_256(b: &mut Bencher) {
    bench_fasthash::<mum::MumHash>(b, 256);
}
#[bench]
fn bench_mum_hash_key_512(b: &mut Bencher) {
    bench_fasthash::<mum::MumHash>(b, 512);
}
#[bench]
fn bench_murmur_key_16(b: &mut Bencher) {
    bench_fasthash::<murmur::Murmur>(b, 16);
}
#[bench]
fn bench_murmur_key_32(b: &mut Bencher) {
    bench_fasthash::<murmur::Murmur>(b, 32);
}
#[bench]
fn bench_murmur_key_64(b: &mut Bencher) {
    bench_fasthash::<murmur::Murmur>(b, 64);
}
#[bench]
fn bench_murmur_key_128(b: &mut Bencher) {
    bench_fasthash::<murmur::Murmur>(b, 128);
}
#[bench]
fn bench_murmur_key_256(b: &mut Bencher) {
    bench_fasthash::<murmur::Murmur>(b, 256);
}
#[bench]
fn bench_murmur_key_512(b: &mut Bencher) {
    bench_fasthash::<murmur::Murmur>(b, 512);
}
#[bench]
fn bench_murmur_aligned_key_16(b: &mut Bencher) {
    bench_fasthash::<murmur::MurmurAligned>(b, 16);
}
#[bench]
fn bench_murmur_aligned_key_32(b: &mut Bencher) {
    bench_fasthash::<murmur::MurmurAligned>(b, 32);
}
#[bench]
fn bench_murmur_aligned_key_64(b: &mut Bencher) {
    bench_fasthash::<murmur::MurmurAligned>(b, 64);
}
#[bench]
fn bench_murmur_aligned_key_128(b: &mut Bencher) {
    bench_fasthash::<murmur::MurmurAligned>(b, 128);
}
#[bench]
fn bench_murmur_aligned_key_256(b: &mut Bencher) {
    bench_fasthash::<murmur::MurmurAligned>(b, 256);
}
#[bench]
fn bench_murmur_aligned_key_512(b: &mut Bencher) {
    bench_fasthash::<murmur::MurmurAligned>(b, 512);
}
#[bench]
fn bench_murmur2_key_16(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2>(b, 16);
}
#[bench]
fn bench_murmur2_key_32(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2>(b, 32);
}
#[bench]
fn bench_murmur2_key_64(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2>(b, 64);
}
#[bench]
fn bench_murmur2_key_128(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2>(b, 128);
}
#[bench]
fn bench_murmur2_key_256(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2>(b, 256);
}
#[bench]
fn bench_murmur2_key_512(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2>(b, 512);
}
#[bench]
fn bench_murmur2_a_key_16(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2A>(b, 16);
}
#[bench]
fn bench_murmur2_a_key_32(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2A>(b, 32);
}
#[bench]
fn bench_murmur2_a_key_64(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2A>(b, 64);
}
#[bench]
fn bench_murmur2_a_key_128(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2A>(b, 128);
}
#[bench]
fn bench_murmur2_a_key_256(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2A>(b, 256);
}
#[bench]
fn bench_murmur2_a_key_512(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2A>(b, 512);
}
#[bench]
fn bench_murmur_neutral2_key_16(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurNeutral2>(b, 16);
}
#[bench]
fn bench_murmur_neutral2_key_32(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurNeutral2>(b, 32);
}
#[bench]
fn bench_murmur_neutral2_key_64(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurNeutral2>(b, 64);
}
#[bench]
fn bench_murmur_neutral2_key_128(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurNeutral2>(b, 128);
}
#[bench]
fn bench_murmur_neutral2_key_256(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurNeutral2>(b, 256);
}
#[bench]
fn bench_murmur_neutral2_key_512(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurNeutral2>(b, 512);
}
#[bench]
fn bench_murmur_aligned2_key_16(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurAligned2>(b, 16);
}
#[bench]
fn bench_murmur_aligned2_key_32(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurAligned2>(b, 32);
}
#[bench]
fn bench_murmur_aligned2_key_64(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurAligned2>(b, 64);
}
#[bench]
fn bench_murmur_aligned2_key_128(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurAligned2>(b, 128);
}
#[bench]
fn bench_murmur_aligned2_key_256(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurAligned2>(b, 256);
}
#[bench]
fn bench_murmur_aligned2_key_512(b: &mut Bencher) {
    bench_fasthash::<murmur2::MurmurAligned2>(b, 512);
}
#[bench]
fn bench_murmur2_x64_64_key_16(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x64_64>(b, 16);
}
#[bench]
fn bench_murmur2_x64_64_key_32(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x64_64>(b, 32);
}
#[bench]
fn bench_murmur2_x64_64_key_64(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x64_64>(b, 64);
}
#[bench]
fn bench_murmur2_x64_64_key_128(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x64_64>(b, 128);
}
#[bench]
fn bench_murmur2_x64_64_key_256(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x64_64>(b, 256);
}
#[bench]
fn bench_murmur2_x64_64_key_512(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x64_64>(b, 512);
}
#[bench]
fn bench_murmur2_x86_64_key_16(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x86_64>(b, 16);
}
#[bench]
fn bench_murmur2_x86_64_key_32(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x86_64>(b, 32);
}
#[bench]
fn bench_murmur2_x86_64_key_64(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x86_64>(b, 64);
}
#[bench]
fn bench_murmur2_x86_64_key_128(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x86_64>(b, 128);
}
#[bench]
fn bench_murmur2_x86_64_key_256(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x86_64>(b, 256);
}
#[bench]
fn bench_murmur2_x86_64_key_512(b: &mut Bencher) {
    bench_fasthash::<murmur2::Murmur2_x86_64>(b, 512);
}
#[bench]
fn bench_murmur3_x86_32_key_16(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_32>(b, 16);
}
#[bench]
fn bench_murmur3_x86_32_key_32(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_32>(b, 32);
}
#[bench]
fn bench_murmur3_x86_32_key_64(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_32>(b, 64);
}
#[bench]
fn bench_murmur3_x86_32_key_128(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_32>(b, 128);
}
#[bench]
fn bench_murmur3_x86_32_key_256(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_32>(b, 256);
}
#[bench]
fn bench_murmur3_x86_32_key_512(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_32>(b, 512);
}
#[bench]
fn bench_murmur3_x86_128_key_16(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_128>(b, 16);
}
#[bench]
fn bench_murmur3_x86_128_key_32(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_128>(b, 32);
}
#[bench]
fn bench_murmur3_x86_128_key_64(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_128>(b, 64);
}
#[bench]
fn bench_murmur3_x86_128_key_128(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_128>(b, 128);
}
#[bench]
fn bench_murmur3_x86_128_key_256(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_128>(b, 256);
}
#[bench]
fn bench_murmur3_x86_128_key_512(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x86_128>(b, 512);
}
#[bench]
fn bench_murmur3_x64_128_key_16(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x64_128>(b, 16);
}
#[bench]
fn bench_murmur3_x64_128_key_32(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x64_128>(b, 32);
}
#[bench]
fn bench_murmur3_x64_128_key_64(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x64_128>(b, 64);
}
#[bench]
fn bench_murmur3_x64_128_key_128(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x64_128>(b, 128);
}
#[bench]
fn bench_murmur3_x64_128_key_256(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x64_128>(b, 256);
}
#[bench]
fn bench_murmur3_x64_128_key_512(b: &mut Bencher) {
    bench_fasthash::<murmur3::Murmur3_x64_128>(b, 512);
}
#[bench]
fn bench_sea_hash_key_16(b: &mut Bencher) {
    bench_fasthash::<sea::SeaHash>(b, 16);
}
#[bench]
fn bench_sea_hash_key_32(b: &mut Bencher) {
    bench_fasthash::<sea::SeaHash>(b, 32);
}
#[bench]
fn bench_sea_hash_key_64(b: &mut Bencher) {
    bench_fasthash::<sea::SeaHash>(b, 64);
}
#[bench]
fn bench_sea_hash_key_128(b: &mut Bencher) {
    bench_fasthash::<sea::SeaHash>(b, 128);
}
#[bench]
fn bench_sea_hash_key_256(b: &mut Bencher) {
    bench_fasthash::<sea::SeaHash>(b, 256);
}
#[bench]
fn bench_sea_hash_key_512(b: &mut Bencher) {
    bench_fasthash::<sea::SeaHash>(b, 512);
}
#[bench]
fn bench_spooky_hash32_key_16(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash32>(b, 16);
}
#[bench]
fn bench_spooky_hash32_key_32(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash32>(b, 32);
}
#[bench]
fn bench_spooky_hash32_key_64(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash32>(b, 64);
}
#[bench]
fn bench_spooky_hash32_key_128(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash32>(b, 128);
}
#[bench]
fn bench_spooky_hash32_key_256(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash32>(b, 256);
}
#[bench]
fn bench_spooky_hash32_key_512(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash32>(b, 512);
}
#[bench]
fn bench_spooky_hash64_key_16(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash64>(b, 16);
}
#[bench]
fn bench_spooky_hash64_key_32(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash64>(b, 32);
}
#[bench]
fn bench_spooky_hash64_key_64(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash64>(b, 64);
}
#[bench]
fn bench_spooky_hash64_key_128(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash64>(b, 128);
}
#[bench]
fn bench_spooky_hash64_key_256(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash64>(b, 256);
}
#[bench]
fn bench_spooky_hash64_key_512(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash64>(b, 512);
}
#[bench]
fn bench_spooky_hash128_key_16(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash128>(b, 16);
}
#[bench]
fn bench_spooky_hash128_key_32(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash128>(b, 32);
}
#[bench]
fn bench_spooky_hash128_key_64(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash128>(b, 64);
}
#[bench]
fn bench_spooky_hash128_key_128(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash128>(b, 128);
}
#[bench]
fn bench_spooky_hash128_key_256(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash128>(b, 256);
}
#[bench]
fn bench_spooky_hash128_key_512(b: &mut Bencher) {
    bench_fasthash::<spooky::SpookyHash128>(b, 512);
}
#[bench]
fn bench_t1ha0_32_le_key_16(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Le>(b, 16);
}
#[bench]
fn bench_t1ha0_32_le_key_32(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Le>(b, 32);
}
#[bench]
fn bench_t1ha0_32_le_key_64(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Le>(b, 64);
}
#[bench]
fn bench_t1ha0_32_le_key_128(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Le>(b, 128);
}
#[bench]
fn bench_t1ha0_32_le_key_256(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Le>(b, 256);
}
#[bench]
fn bench_t1ha0_32_le_key_512(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Le>(b, 512);
}
#[bench]
fn bench_t1ha0_32_be_key_16(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Be>(b, 16);
}
#[bench]
fn bench_t1ha0_32_be_key_32(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Be>(b, 32);
}
#[bench]
fn bench_t1ha0_32_be_key_64(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Be>(b, 64);
}
#[bench]
fn bench_t1ha0_32_be_key_128(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Be>(b, 128);
}
#[bench]
fn bench_t1ha0_32_be_key_256(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Be>(b, 256);
}
#[bench]
fn bench_t1ha0_32_be_key_512(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha0_32Be>(b, 512);
}
#[bench]
fn bench_t1ha1_64_le_key_16(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Le>(b, 16);
}
#[bench]
fn bench_t1ha1_64_le_key_32(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Le>(b, 32);
}
#[bench]
fn bench_t1ha1_64_le_key_64(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Le>(b, 64);
}
#[bench]
fn bench_t1ha1_64_le_key_128(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Le>(b, 128);
}
#[bench]
fn bench_t1ha1_64_le_key_256(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Le>(b, 256);
}
#[bench]
fn bench_t1ha1_64_le_key_512(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Le>(b, 512);
}
#[bench]
fn bench_t1ha1_64_be_key_16(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Be>(b, 16);
}
#[bench]
fn bench_t1ha1_64_be_key_32(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Be>(b, 32);
}
#[bench]
fn bench_t1ha1_64_be_key_64(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Be>(b, 64);
}
#[bench]
fn bench_t1ha1_64_be_key_128(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Be>(b, 128);
}
#[bench]
fn bench_t1ha1_64_be_key_256(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Be>(b, 256);
}
#[bench]
fn bench_t1ha1_64_be_key_512(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha1_64Be>(b, 512);
}
#[bench]
fn bench_t1ha2_64_key_16(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha2_64>(b, 16);
}
#[bench]
fn bench_t1ha2_64_key_32(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha2_64>(b, 32);
}
#[bench]
fn bench_t1ha2_64_key_64(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha2_64>(b, 64);
}
#[bench]
fn bench_t1ha2_64_key_128(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha2_64>(b, 128);
}
#[bench]
fn bench_t1ha2_64_key_256(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha2_64>(b, 256);
}
#[bench]
fn bench_t1ha2_64_key_512(b: &mut Bencher) {
    bench_fasthash::<t1ha::T1ha2_64>(b, 512);
}
#[bench]
fn bench_x_x_hash32_key_16(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash32>(b, 16);
}
#[bench]
fn bench_x_x_hash32_key_32(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash32>(b, 32);
}
#[bench]
fn bench_x_x_hash32_key_64(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash32>(b, 64);
}
#[bench]
fn bench_x_x_hash32_key_128(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash32>(b, 128);
}
#[bench]
fn bench_x_x_hash32_key_256(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash32>(b, 256);
}
#[bench]
fn bench_x_x_hash32_key_512(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash32>(b, 512);
}
#[bench]
fn bench_x_x_hash64_key_16(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash64>(b, 16);
}
#[bench]
fn bench_x_x_hash64_key_32(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash64>(b, 32);
}
#[bench]
fn bench_x_x_hash64_key_64(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash64>(b, 64);
}
#[bench]
fn bench_x_x_hash64_key_128(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash64>(b, 128);
}
#[bench]
fn bench_x_x_hash64_key_256(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash64>(b, 256);
}
#[bench]
fn bench_x_x_hash64_key_512(b: &mut Bencher) {
    bench_fasthash::<xx::XXHash64>(b, 512);
}
#[bench]
fn bench_sip_hasher_key_16(b: &mut Bencher) {
    bench_hasher::<SipHasher>(b, 16);
}
#[bench]
fn bench_sip_hasher_key_32(b: &mut Bencher) {
    bench_hasher::<SipHasher>(b, 32);
}
#[bench]
fn bench_sip_hasher_key_64(b: &mut Bencher) {
    bench_hasher::<SipHasher>(b, 64);
}
#[bench]
fn bench_sip_hasher_key_128(b: &mut Bencher) {
    bench_hasher::<SipHasher>(b, 128);
}
#[bench]
fn bench_sip_hasher_key_256(b: &mut Bencher) {
    bench_hasher::<SipHasher>(b, 256);
}
#[bench]
fn bench_sip_hasher_key_512(b: &mut Bencher) {
    bench_hasher::<SipHasher>(b, 512);
}
#[bench]
fn bench_fnv_hasher_key_16(b: &mut Bencher) {
    bench_hasher::<FnvHasher>(b, 16);
}
#[bench]
fn bench_fnv_hasher_key_32(b: &mut Bencher) {
    bench_hasher::<FnvHasher>(b, 32);
}
#[bench]
fn bench_fnv_hasher_key_64(b: &mut Bencher) {
    bench_hasher::<FnvHasher>(b, 64);
}
#[bench]
fn bench_fnv_hasher_key_128(b: &mut Bencher) {
    bench_hasher::<FnvHasher>(b, 128);
}
#[bench]
fn bench_fnv_hasher_key_256(b: &mut Bencher) {
    bench_hasher::<FnvHasher>(b, 256);
}
#[bench]
fn bench_fnv_hasher_key_512(b: &mut Bencher) {
    bench_hasher::<FnvHasher>(b, 512);
}
#[bench]
fn bench_sea_hasher_key_16(b: &mut Bencher) {
    bench_hasher::<SeaHasher>(b, 16);
}
#[bench]
fn bench_sea_hasher_key_32(b: &mut Bencher) {
    bench_hasher::<SeaHasher>(b, 32);
}
#[bench]
fn bench_sea_hasher_key_64(b: &mut Bencher) {
    bench_hasher::<SeaHasher>(b, 64);
}
#[bench]
fn bench_sea_hasher_key_128(b: &mut Bencher) {
    bench_hasher::<SeaHasher>(b, 128);
}
#[bench]
fn bench_sea_hasher_key_256(b: &mut Bencher) {
    bench_hasher::<SeaHasher>(b, 256);
}
#[bench]
fn bench_sea_hasher_key_512(b: &mut Bencher) {
    bench_hasher::<SeaHasher>(b, 512);
}
#[bench]
fn bench_spooky_hasher_key_16(b: &mut Bencher) {
    bench_hasher::<SpookyHasher>(b, 16);
}
#[bench]
fn bench_spooky_hasher_key_32(b: &mut Bencher) {
    bench_hasher::<SpookyHasher>(b, 32);
}
#[bench]
fn bench_spooky_hasher_key_64(b: &mut Bencher) {
    bench_hasher::<SpookyHasher>(b, 64);
}
#[bench]
fn bench_spooky_hasher_key_128(b: &mut Bencher) {
    bench_hasher::<SpookyHasher>(b, 128);
}
#[bench]
fn bench_spooky_hasher_key_256(b: &mut Bencher) {
    bench_hasher::<SpookyHasher>(b, 256);
}
#[bench]
fn bench_spooky_hasher_key_512(b: &mut Bencher) {
    bench_hasher::<SpookyHasher>(b, 512);
}
#[bench]
fn bench_spooky_hasher_ext_key_16(b: &mut Bencher) {
    bench_hasher::<SpookyHasherExt>(b, 16);
}
#[bench]
fn bench_spooky_hasher_ext_key_32(b: &mut Bencher) {
    bench_hasher::<SpookyHasherExt>(b, 32);
}
#[bench]
fn bench_spooky_hasher_ext_key_64(b: &mut Bencher) {
    bench_hasher::<SpookyHasherExt>(b, 64);
}
#[bench]
fn bench_spooky_hasher_ext_key_128(b: &mut Bencher) {
    bench_hasher::<SpookyHasherExt>(b, 128);
}
#[bench]
fn bench_spooky_hasher_ext_key_256(b: &mut Bencher) {
    bench_hasher::<SpookyHasherExt>(b, 256);
}
#[bench]
fn bench_spooky_hasher_ext_key_512(b: &mut Bencher) {
    bench_hasher::<SpookyHasherExt>(b, 512);
}
#[bench]
fn bench_t1ha_hasher_key_16(b: &mut Bencher) {
    bench_hasher::<T1haHasher>(b, 16);
}
#[bench]
fn bench_t1ha_hasher_key_32(b: &mut Bencher) {
    bench_hasher::<T1haHasher>(b, 32);
}
#[bench]
fn bench_t1ha_hasher_key_64(b: &mut Bencher) {
    bench_hasher::<T1haHasher>(b, 64);
}
#[bench]
fn bench_t1ha_hasher_key_128(b: &mut Bencher) {
    bench_hasher::<T1haHasher>(b, 128);
}
#[bench]
fn bench_t1ha_hasher_key_256(b: &mut Bencher) {
    bench_hasher::<T1haHasher>(b, 256);
}
#[bench]
fn bench_t1ha_hasher_key_512(b: &mut Bencher) {
    bench_hasher::<T1haHasher>(b, 512);
}
#[bench]
fn bench_x_x_hasher_key_16(b: &mut Bencher) {
    bench_hasher::<XXHasher>(b, 16);
}
#[bench]
fn bench_x_x_hasher_key_32(b: &mut Bencher) {
    bench_hasher::<XXHasher>(b, 32);
}
#[bench]
fn bench_x_x_hasher_key_64(b: &mut Bencher) {
    bench_hasher::<XXHasher>(b, 64);
}
#[bench]
fn bench_x_x_hasher_key_128(b: &mut Bencher) {
    bench_hasher::<XXHasher>(b, 128);
}
#[bench]
fn bench_x_x_hasher_key_256(b: &mut Bencher) {
    bench_hasher::<XXHasher>(b, 256);
}
#[bench]
fn bench_x_x_hasher_key_512(b: &mut Bencher) {
    bench_hasher::<XXHasher>(b, 512);
}
#[bench]
fn bench_city_hasher_key_16(b: &mut Bencher) {
    bench_buf_hasher::<CityHasher>(b, 16);
}
#[bench]
fn bench_city_hasher_key_32(b: &mut Bencher) {
    bench_buf_hasher::<CityHasher>(b, 32);
}
#[bench]
fn bench_city_hasher_key_64(b: &mut Bencher) {
    bench_buf_hasher::<CityHasher>(b, 64);
}
#[bench]
fn bench_city_hasher_key_128(b: &mut Bencher) {
    bench_buf_hasher::<CityHasher>(b, 128);
}
#[bench]
fn bench_city_hasher_key_256(b: &mut Bencher) {
    bench_buf_hasher::<CityHasher>(b, 256);
}
#[bench]
fn bench_city_hasher_key_512(b: &mut Bencher) {
    bench_buf_hasher::<CityHasher>(b, 512);
}
#[bench]
fn bench_city_hasher_ext_key_16(b: &mut Bencher) {
    bench_buf_hasher::<CityHasherExt>(b, 16);
}
#[bench]
fn bench_city_hasher_ext_key_32(b: &mut Bencher) {
    bench_buf_hasher::<CityHasherExt>(b, 32);
}
#[bench]
fn bench_city_hasher_ext_key_64(b: &mut Bencher) {
    bench_buf_hasher::<CityHasherExt>(b, 64);
}
#[bench]
fn bench_city_hasher_ext_key_128(b: &mut Bencher) {
    bench_buf_hasher::<CityHasherExt>(b, 128);
}
#[bench]
fn bench_city_hasher_ext_key_256(b: &mut Bencher) {
    bench_buf_hasher::<CityHasherExt>(b, 256);
}
#[bench]
fn bench_city_hasher_ext_key_512(b: &mut Bencher) {
    bench_buf_hasher::<CityHasherExt>(b, 512);
}
#[bench]
fn bench_farm_hasher_key_16(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasher>(b, 16);
}
#[bench]
fn bench_farm_hasher_key_32(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasher>(b, 32);
}
#[bench]
fn bench_farm_hasher_key_64(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasher>(b, 64);
}
#[bench]
fn bench_farm_hasher_key_128(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasher>(b, 128);
}
#[bench]
fn bench_farm_hasher_key_256(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasher>(b, 256);
}
#[bench]
fn bench_farm_hasher_key_512(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasher>(b, 512);
}
#[bench]
fn bench_farm_hasher_ext_key_16(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasherExt>(b, 16);
}
#[bench]
fn bench_farm_hasher_ext_key_32(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasherExt>(b, 32);
}
#[bench]
fn bench_farm_hasher_ext_key_64(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasherExt>(b, 64);
}
#[bench]
fn bench_farm_hasher_ext_key_128(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasherExt>(b, 128);
}
#[bench]
fn bench_farm_hasher_ext_key_256(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasherExt>(b, 256);
}
#[bench]
fn bench_farm_hasher_ext_key_512(b: &mut Bencher) {
    bench_buf_hasher::<FarmHasherExt>(b, 512);
}
#[bench]
fn bench_lookup3_hasher_key_16(b: &mut Bencher) {
    bench_buf_hasher::<Lookup3Hasher>(b, 16);
}
#[bench]
fn bench_lookup3_hasher_key_32(b: &mut Bencher) {
    bench_buf_hasher::<Lookup3Hasher>(b, 32);
}
#[bench]
fn bench_lookup3_hasher_key_64(b: &mut Bencher) {
    bench_buf_hasher::<Lookup3Hasher>(b, 64);
}
#[bench]
fn bench_lookup3_hasher_key_128(b: &mut Bencher) {
    bench_buf_hasher::<Lookup3Hasher>(b, 128);
}
#[bench]
fn bench_lookup3_hasher_key_256(b: &mut Bencher) {
    bench_buf_hasher::<Lookup3Hasher>(b, 256);
}
#[bench]
fn bench_lookup3_hasher_key_512(b: &mut Bencher) {
    bench_buf_hasher::<Lookup3Hasher>(b, 512);
}
#[bench]
fn bench_metro_hasher_key_16(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasher>(b, 16);
}
#[bench]
fn bench_metro_hasher_key_32(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasher>(b, 32);
}
#[bench]
fn bench_metro_hasher_key_64(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasher>(b, 64);
}
#[bench]
fn bench_metro_hasher_key_128(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasher>(b, 128);
}
#[bench]
fn bench_metro_hasher_key_256(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasher>(b, 256);
}
#[bench]
fn bench_metro_hasher_key_512(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasher>(b, 512);
}
#[bench]
fn bench_metro_hasher_ext_key_16(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasherExt>(b, 16);
}
#[bench]
fn bench_metro_hasher_ext_key_32(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasherExt>(b, 32);
}
#[bench]
fn bench_metro_hasher_ext_key_64(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasherExt>(b, 64);
}
#[bench]
fn bench_metro_hasher_ext_key_128(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasherExt>(b, 128);
}
#[bench]
fn bench_metro_hasher_ext_key_256(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasherExt>(b, 256);
}
#[bench]
fn bench_metro_hasher_ext_key_512(b: &mut Bencher) {
    bench_buf_hasher::<MetroHasherExt>(b, 512);
}
#[bench]
fn bench_mum_hasher_key_16(b: &mut Bencher) {
    bench_buf_hasher::<MumHasher>(b, 16);
}
#[bench]
fn bench_mum_hasher_key_32(b: &mut Bencher) {
    bench_buf_hasher::<MumHasher>(b, 32);
}
#[bench]
fn bench_mum_hasher_key_64(b: &mut Bencher) {
    bench_buf_hasher::<MumHasher>(b, 64);
}
#[bench]
fn bench_mum_hasher_key_128(b: &mut Bencher) {
    bench_buf_hasher::<MumHasher>(b, 128);
}
#[bench]
fn bench_mum_hasher_key_256(b: &mut Bencher) {
    bench_buf_hasher::<MumHasher>(b, 256);
}
#[bench]
fn bench_mum_hasher_key_512(b: &mut Bencher) {
    bench_buf_hasher::<MumHasher>(b, 512);
}
#[bench]
fn bench_murmur_hasher_key_16(b: &mut Bencher) {
    bench_buf_hasher::<MurmurHasher>(b, 16);
}
#[bench]
fn bench_murmur_hasher_key_32(b: &mut Bencher) {
    bench_buf_hasher::<MurmurHasher>(b, 32);
}
#[bench]
fn bench_murmur_hasher_key_64(b: &mut Bencher) {
    bench_buf_hasher::<MurmurHasher>(b, 64);
}
#[bench]
fn bench_murmur_hasher_key_128(b: &mut Bencher) {
    bench_buf_hasher::<MurmurHasher>(b, 128);
}
#[bench]
fn bench_murmur_hasher_key_256(b: &mut Bencher) {
    bench_buf_hasher::<MurmurHasher>(b, 256);
}
#[bench]
fn bench_murmur_hasher_key_512(b: &mut Bencher) {
    bench_buf_hasher::<MurmurHasher>(b, 512);
}
#[bench]
fn bench_murmur2_hasher_key_16(b: &mut Bencher) {
    bench_buf_hasher::<Murmur2Hasher>(b, 16);
}
#[bench]
fn bench_murmur2_hasher_key_32(b: &mut Bencher) {
    bench_buf_hasher::<Murmur2Hasher>(b, 32);
}
#[bench]
fn bench_murmur2_hasher_key_64(b: &mut Bencher) {
    bench_buf_hasher::<Murmur2Hasher>(b, 64);
}
#[bench]
fn bench_murmur2_hasher_key_128(b: &mut Bencher) {
    bench_buf_hasher::<Murmur2Hasher>(b, 128);
}
#[bench]
fn bench_murmur2_hasher_key_256(b: &mut Bencher) {
    bench_buf_hasher::<Murmur2Hasher>(b, 256);
}
#[bench]
fn bench_murmur2_hasher_key_512(b: &mut Bencher) {
    bench_buf_hasher::<Murmur2Hasher>(b, 512);
}
#[bench]
fn bench_murmur3_hasher_key_16(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3Hasher>(b, 16);
}
#[bench]
fn bench_murmur3_hasher_key_32(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3Hasher>(b, 32);
}
#[bench]
fn bench_murmur3_hasher_key_64(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3Hasher>(b, 64);
}
#[bench]
fn bench_murmur3_hasher_key_128(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3Hasher>(b, 128);
}
#[bench]
fn bench_murmur3_hasher_key_256(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3Hasher>(b, 256);
}
#[bench]
fn bench_murmur3_hasher_key_512(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3Hasher>(b, 512);
}
#[bench]
fn bench_murmur3_hasher_ext_key_16(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3HasherExt>(b, 16);
}
#[bench]
fn bench_murmur3_hasher_ext_key_32(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3HasherExt>(b, 32);
}
#[bench]
fn bench_murmur3_hasher_ext_key_64(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3HasherExt>(b, 64);
}
#[bench]
fn bench_murmur3_hasher_ext_key_128(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3HasherExt>(b, 128);
}
#[bench]
fn bench_murmur3_hasher_ext_key_256(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3HasherExt>(b, 256);
}
#[bench]
fn bench_murmur3_hasher_ext_key_512(b: &mut Bencher) {
    bench_buf_hasher::<Murmur3HasherExt>(b, 512);
}