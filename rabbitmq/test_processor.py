"""Test script to demonstrate user profile data parsing."""

from user_profile_processor import UserProfileProcessor

# Example data from substrate storage
example_substrate_data = [
    [
        ["5GdqPwn1Ley9jmxSk52cHFqWKFc864XfaBMffXo1jUtwNmx3"],
        "bafkreicx4yjeyjgazxz7tvzuyhvl565wka2oqa566pbsc5u5tfvnlk3vzq"
    ],
    [
        ["5GeakAuWoJDYhcGCXoQaqsGGTF7BSdef1hmCraNvpHGL33zB"],
        "bafkreicse3wfmej6vxiqo5yxur4suex5dixiv6bgxz5z7kaiwgt3l3m5km"
    ],
    [
        ["5HoreGVb17XhY3wanDvzoAWS7yHYbc5uMteXqRNTiZ6Txkqq"],
        "bafkreia7igb6nzbwj577da53nc6ezxrpkgwjxqhiemcjanqth7yqpzzj3e"
    ],
    [
        ["5DCgTSdRbhoS1U3eDtYv4asT9Ljg8yBmendVKMF94tWtM7af"],
        "bafkreic4r6l76bom7dj7yowgbhbw24iekn3m3souizhpjyw3fw6k5lxkta"
    ],
    [
        ["5E9d3J4gDFqWdiDKiWu4gucwPUYC9rh2MbL2LezyhDjT652d"],
        "bafybeif3ygnubirrdocpbnt6gd3tzs4yml63vdx63czpjbpudzzxuw4giq"
    ],
    [
        ["5F479YjYTKz9j9y9QKwogaq7DNHaFHFepNeRx4uaHEpuekkM"],
        "bafkreiesgmo7b4zsn2ickf4ii2epp7a3bxvipbmwdrmdivh6ggu3y46kmu"
    ],
    [
        ["5CRyFwmSHJC7EeGLGbU1G8ycuoxu8sQxExhfBhkwNPtQU5n2"],
        "bafkreiagy564p7kfz7w52lx3rmngkim5hatvdq4dj6c5oyuu4ha7s2vy64"
    ],
    [
        ["5DtN5wuzKdCSH4oM8tuHZF7yenCdmSVr1LKShNXbzsd1SC8Q"],
        "bafkreidffgcptlvj6helpgl5wxi2isurqec4ffczlo3f24fhazhajt7z6q"
    ],
    [
        ["5FbiUU9cUUCZ653LhKT6r7fhyv2y6zMeyN78P2LwwmjbTX3y"],
        "bafkreidozajqjkmk5u2uzpf52rsdnotx3zrsym2bu5b6uiektdxihghhge"
    ],
    [
        ["5Gn4Pp8fYdipwASKNQS6VnxC7h6DZj1AHZaFEQHyDR1mswV1"],
        "bafybeibcgkwmadqigc4kijurt5zjavbkn3l2ppqkon7qqn6lisl25tigsa"
    ]
]

def test_parsing():
    """Test the parsing of substrate data."""
    processor = UserProfileProcessor()
    
    print("Testing user profile data parsing...")
    print("=" * 60)
    
    parsed_profiles = processor.parse_user_profile_data(example_substrate_data)
    
    print(f"\nParsed {len(parsed_profiles)} profiles:\n")
    
    for i, profile in enumerate(parsed_profiles, 1):
        print(f"Profile {i}:")
        print(f"  Account: {profile['account']}")
        print(f"  CID: {profile['cid']}")
        print(f"  Timestamp: {profile['timestamp']}")
        print()
    
    print("=" * 60)
    print("\nExample JSON that would be sent to RabbitMQ queue:")
    print("=" * 60)
    
    import json
    for profile in parsed_profiles[:3]:  # Show first 3 as examples
        print(json.dumps(profile, indent=2))
        print("-" * 40)

if __name__ == "__main__":
    test_parsing() 