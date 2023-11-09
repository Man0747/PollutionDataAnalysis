def isSubsetSum(arr, n, S, current_sum=0, index=0, subset=[]):
    if current_sum == S:
        print("Solution exists:", subset)
        return True

    if index == n:
        return False

    # Include the current element in the subset
    subset.append(arr[index])
    if isSubsetSum(arr, n, S, current_sum + arr[index], index + 1, subset):
        subset.pop()  # Backtrack if the subset is a solution

    # Exclude the current element from the subset
    if isSubsetSum(arr, n, S, current_sum, index + 1, subset):
        subset.pop()  # Backtrack if the subset is a solution

    return False


input_list = input("Enter a list, for example [1,2,3,4]- ")
arr = [int(x) for x in input_list.strip("[]").split(",")]
S = int(input("Enter sum: "))

if isSubsetSum(arr, len(arr), S):
    print("Solution exists")
else:
    print("No solution exists")