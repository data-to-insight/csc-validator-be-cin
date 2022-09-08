class FailedPoints:
    def __init__(self) -> None:
        self.failed_points = []
        self.failing_relatives = []

    def push(self, failing_points):
        """list of single failed points"""
        self.failed_points.extend(failing_points)

    def push_relatives(self, failing_relatives):
        """add a list of tuples. Each tuple should contain all linked failed points for one instance
        To use this,
        - generate a list of PointLocators for each table involved.
        - zip the lists and unpack the zip object into a list for example [*zip(lst1, lst2, lst3)]
        - Add your failed points to the mother tracker by passing the resulting list as an argument into this function.
        """

        # link all locations flagged by the same instance of an error.
        self.failed_relatives.extend(failing_relatives)
