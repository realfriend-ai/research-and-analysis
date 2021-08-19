from bson import ObjectId

luke_fb_user_ids = ['5d99c657f4789a0017ecabf2', '5d5e17ae22c8ee003473df3f', '5d138e2115198a001779a43c',
                    '5c76704525a2500016303f49', '5ec3b503510131bdf32b8a85', '5ec27d9a4055061fddbf7c9e',
                    '5de7d1f3ee04d0003971d8ff', '5e0dc9fde67a9000173ae1d8', '5cb5e62accca7200172b0cbc',
                    '5bed83ec04274e0013b8c005', '608771ae2613c3e7df89115a', '600ba1ba161f2232d9055ad1',
                    '5cb5e9b49ffed90017d4a280', '5de112d864ebd1004552ecca', '5f44bb7b64250e6388ae4e48',
                    '5dbff11b7e3994001775cd27', '5d31cf8675b8f50017123dad', '5de041e3843b7c001757b363',
                    '5dd684ba8cae1200174d6aa3', '5de8aa7d4a1d48003479a6f7', '5deeb9f941f7920034c627cf',
                    '5f33cc2254feb44973c47dbc', '5cd348d6206f3a0017ee99b8', '605248f6eec61b1f8f935314',
                    '5ce65b960572220017680733', '5c489564b28eac0016ec832e', '5de0074ea12cab0034cad4f6', ''
                                                                                                        '5cc86477146c7600179fa577',
                    '5ca26a81d6d7ae00171c28e8', '5cd348d6206f3a0017ee99b8']

luke_fb_user_ids = list(map(lambda x: ObjectId(x), luke_fb_user_ids))

bug_users = ['6072319fd77705c1f86ef3e4',
             '60b50f2c32955a6581c9dc7e',
             '60c09b356a3b6e3b07f4f422',
             '60d66750969823b0446cde5e',
             '60d9c402383b8276e4002258',
             '60da45a16c0a36c51b3402a9',
             '60dcb0fefed4840b917ac02d']


bug_users_ids = list(map(lambda x: ObjectId(x), bug_users))
