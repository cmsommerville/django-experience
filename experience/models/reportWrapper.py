import datetime

class ReportWrapper():

    def __init__(self, reportSetID, reportID, reportKey, reportKeyType, status, *args, **kwargs):

        self.reportSetID = reportSetID
        self.reportID = reportID
        self.report = kwargs.get("report")

        self.reportKey = self.minifyKeyDisplay(reportKey)
        self.reportKeyType = reportKeyType

        self.reportLink = kwargs.get("reportLink", "")
        self.title = kwargs.get("title", "")
        self.status = status
        self.reason = kwargs.get("reason", "")
        self.error = kwargs.get("error", "")
        self.runTimeStamp = str(datetime.datetime.now())

    def minifyKeyDisplay(self, input, front = 3, back = 1):
        if not isinstance(input, list):
            return(input)
        elif len(input) <= front + back:
            return(', '.join(input))
        else:
            return(', '.join(input[0:3]) + ',..., ' + input[-1])

    def display(self):
        output = {
            "status": self.status,
            "log": {
                "reportSetID": self.reportSetID,
                "reportID": self.reportID,
                "reportKey": self.reportKey,
                "reportKeyType": self.reportKeyType,
                "title": self.title,
                "reportLink": self.reportLink,
                "runTimeStamp": self.runTimeStamp,
                "status": self.status,
                "reason": self.reason,
                "error": self.error
            }}

        return(output)


    def set_link(self, link):
        self.reportLink = link

    def set_status(self, status, reason = None, err = None):
        self.status = status

        if reason is not None:
            self.reason = reason

        if err is not None:
            self.err = err
