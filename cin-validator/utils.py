class FailedPoints:
    def __init__(self) -> None:
        self.failed_points = []
        self.failing_relatives = []

    def push(self, failing_points):
        """list of single failed points"""
        self.failed_points.extend(failing_points)

    def push_relatives(self, locators_list):
        """add a list of tuples. Each tuple should contain all linked failed points for one instance
        To use this,
        - generate a list of PointLocators for each column involved.
        -  create a list containing all the locator_lists generated e.g [lst1, lst2, lst3]
        - Add your failed points to the mother tracker by passing that list as an argument into this function.
        """

        # if all the lists are of the same length. Assume that they are 1 to 1 relatives.
        if len({len(i) for i in locators_list}) == 1:
            failing_relatives = [*zip(*locators_list)]

        else:
            raise Exception(
                """locator lists should be the same length. \n
            To resolve this, make sure there are no NaNs in your failing_indices 
            and remove any step that de-duplicates failing_indices. 
            failing_indices are the index positions where the rule fails."""
            )

        # link all locations flagged by the same instance of an error.
        self.failed_relatives.extend(failing_relatives)
