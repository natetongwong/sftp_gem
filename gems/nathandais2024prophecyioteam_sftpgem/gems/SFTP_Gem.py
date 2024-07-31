from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *


class SFTPCopy(ComponentSpec):
    name: str = "SFTPCopy"
    category: str = "Custom"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class SFTPCopyProperties(ComponentProperties):
        # properties for the component with default values
        secretUsername: SecretValue = field(default_factory=list)
        secretPassword: SecretValue = field(default_factory=list)
        hostUrl: str = ""
        sftpFilePath: str = ""
        destinationPath: str = ""

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component
        return Dialog("SFTPCopy") \
            .addElement(ColumnsLayout(gap="1rem", height="100%")
                        .addColumn(StackLayout(direction=("vertical"), gap=("1rem"))
                                .addElement(TitleElement(title="Credentials"))
                                .addElement(
                                StackLayout()
                                    .addElement(
                                    ColumnsLayout(gap="1rem")
                                        .addColumn(SecretBox("Username").bindPlaceholder("username").bindProperty("secretUsername"))
                                        .addColumn(SecretBox("Password").isPassword().bindPlaceholder("password").bindProperty("secretPassword"))
                                )
                                .addElement(TitleElement(title="Source SFTP Details"))
                                .addElement(
                                StackLayout()
                                    .addElement(
                                    ColumnsLayout(gap="1rem")
                                        .addColumn(TextBox("Host").bindPlaceholder("192.168.1.5").bindProperty("hostUrl"))
                                        .addColumn(TextBox("Source File Path On SFTP").bindPlaceholder("/home/user/samples.csv").bindProperty("sftpFilePath"))
                                    )
                                )
                                .addElement(TitleElement(title="Destination Details"))
                                .addElement(
                                    StackLayout()
                                        .addElement(
                                        ColumnsLayout(gap="1rem")
                                            .addColumn(TextBox("Destination Path").bindPlaceholder("dbfs:/FileStore/test/samples.csv").bindProperty("destinationPath"))
                                        )
                                )
                            ),
                        "2fr")
            )

    def validate(self, context: WorkflowContext, component: Component[SFTPCopyProperties]) -> List[Diagnostic]:
        diagnostics = []
        if component.properties.hostUrl is None or component.properties.hostUrl == "":
            diagnostics.append(Diagnostic("properties.hostUrl", "Host URL can not be empty", SeverityLevelEnum.Error))

        if component.properties.sftpFilePath is None or component.properties.sftpFilePath == "":
            diagnostics.append(Diagnostic("properties.sftpFilePath", "SFTP File path can not be empty", SeverityLevelEnum.Error))

        if component.properties.destinationPath is None or component.properties.destinationPath == "":
            diagnostics.append(Diagnostic("properties.destinationPath", "Destionation path can not be empty", SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[SFTPCopyProperties], newState: Component[SFTPCopyProperties]) -> Component[
    SFTPCopyProperties]:
        # Handle changes in the component's state and return the new state
        return newState


    class SFTPCopyCode(ComponentCode):
        def __init__(self, newProps):
            self.props: SFTPCopy.SFTPCopyProperties = newProps

        # def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
        def apply(self, spark: SparkSession):
            import paramiko
            import os

            def download_file_from_sftp(username, password, host, remote_file_path, local_file_path):
                ssh_client: SubstituteDisabled = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                try:
                    ssh_client.connect(host, username=username, password=password)
                    sftp_client = ssh_client.open_sftp()
                    try:
                        sftp_client.get(remote_file_path, local_file_path)
                    finally:
                        sftp_client.close()
                finally:
                    ssh_client.close()

            local_file_path = self.props.sftpFilePath

            local_dir = os.path.dirname(local_file_path)
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)

            dbfs_file_path = self.props.destinationPath

            download_file_from_sftp(username=self.props.secretUsername, password=self.props.secretPassword, host=self.props.hostUrl, remote_file_path=self.props.sftpFilePath, local_file_path=local_file_path)
            
            dbutils.fs.cp(f"file:{local_file_path}", dbfs_file_path)