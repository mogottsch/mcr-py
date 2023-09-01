from package.mcr.label import IntermediateLabel, merge_intermediate_bags


def test_merge_intermediate_bags():
    il1 = IntermediateLabel([1, 1], [1, 1], [1, "a"], 1)
    il2 = IntermediateLabel([2, 2], [2, 2], [2, "b"], 2)
    il3 = IntermediateLabel([2, 1], [2, 1], [2, "c"], 3)

    bag1 = [il1]
    bag2 = [il2, il3]

    merged_bag = merge_intermediate_bags(bag1, bag2)

    assert (
        len(merged_bag) == 1
    ) 
    assert il1 in merged_bag
    assert il3 not in merged_bag
    assert il2 not in merged_bag
