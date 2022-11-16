package abcd.adls;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.*;
import org.apache.commons.cli.*;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;

public class ADLSClientConnector {
    public static DataLakeServiceClient serviceClient;
    public static void GetDataLakeServiceClientAccessKey
            (String accountName, String accountKey){

        StorageSharedKeyCredential sharedKeyCredential =
                new StorageSharedKeyCredential(accountName, accountKey);

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();

        builder.credential(sharedKeyCredential);
        builder.endpoint("https://" + accountName + ".dfs.core.windows.net");

        serviceClient= builder.buildClient();
    }

    public static void GetDataLakeServiceClientServicePrincipal
            (String accountName, String clientId, String clientSecret ,String tenantId) {

        ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .clientSecret(clientSecret)
                .tenantId(tenantId)
                .build();
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();

        builder.credential(clientSecretCredential);
        builder.endpoint("https://" + accountName + ".dfs.core.windows.net");

        serviceClient = builder.buildClient();
    }

    public static void main(String[] args) {
        Options options = new Options();

        Option account_name = new Option("account_name", "account_name", true, "azure storage account name");
        account_name.setRequired(true);
        options.addOption(account_name);

        Option container_name = new Option("container_name", "container_name", true, "Container name within storage account");
        container_name.setRequired(true);
        options.addOption(container_name);

        Option auth_type = new Option("auth_type", "auth_type", true, "Authentication mechanism to use (Account Key / service principal)");
        auth_type.setRequired(true);
        options.addOption(auth_type);

        Option account_key = new Option("account_key", "account_key", true, "Storage Account key to access ADLS Gen2");
        account_key.setRequired(false);
        options.addOption(account_key);

        Option tenant_id = new Option("tenant_id", "tenant_id", true, "tenant_id of the ADLS Gen2 Storage Account(Only for service principal based authentication");
        tenant_id.setRequired(false);
        options.addOption(tenant_id);

        Option client_id = new Option("client_id", "client_id", true, "client_id of the App used to access the ADLS Gen2");
        client_id.setRequired(false);
        options.addOption(client_id);

        Option client_secret = new Option("client_secret", "client_secret", true, "client_secret of the App used to access the ADLS Gen2");
        client_secret.setRequired(false);
        options.addOption(client_secret);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("ADLS-Gen2-Connectivity-Test", options);

            System.exit(1);
        }
        String accountName = cmd.getOptionValue("account_name");
        String containerName = cmd.getOptionValue("container_name");
        String authType = cmd.getOptionValue("auth_type");
        String accountKey = cmd.getOptionValue("account_key");
        String tenantId = cmd.getOptionValue("tenant_id");
        String clientId = cmd.getOptionValue("client_id");
        String clientSecret = cmd.getOptionValue("client_secret");

        System.out.println("Account Name : "+accountName);
        System.out.println("Auth Type : "+authType);
        java.util.List<String> AllowedAuthTypes = new ArrayList<>();
        AllowedAuthTypes.add("service_principal");
        AllowedAuthTypes.add("account_key");

        if (!AllowedAuthTypes.contains(authType) ){
            System.out.println("Invalid auth_type passed as argument.Allowed values ['service_principal','account_key']");
            System.out.println("Exiting..");
            System.exit(-100);
        }
        System.out.println("Testing Connectivity to ADLS Gen2...");
        if (authType.equals("account_key")){
            GetDataLakeServiceClientAccessKey(accountName,accountKey);
        }
        else {
            GetDataLakeServiceClientServicePrincipal(accountName, clientId, clientSecret, tenantId);
        }
        DataLakeFileSystemClient fileSystemClient = serviceClient.getFileSystemClient(containerName);
        System.out.println("Creating directory under the container");
        DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient("iwx_test_connectivity");
        try {
            /*
             * Create the iwx_test_connectivity directory.
             */
            directoryClient.create(true);
            System.out.println("Successfully created/overwrote the iwx_test_connectivity directory under the given container!");
        }
        catch (Exception ex){
            System.out.println("Sample Directory Creation Failed!");
            System.err.print(ex.getMessage());

        }
        DataLakeFileClient fileClient = directoryClient.getFileClient("sample.txt");
        System.out.println("Trying to upload a sample file to container "+containerName+"...");
        try {
            String data = "Hello world!";
            InputStream dataStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));

            /*
             * Create the file with string (plain text) content.
             */
            fileClient.create(true);
            fileClient.append(dataStream, 0, data.length());
            fileClient.flush(data.length(),true);
            dataStream.close();

            System.out.println("Creation of sample file succeeded!");
        } catch (java.io.UncheckedIOException | java.io.IOException ex) {
            System.err.printf("Failed to upload file %s%n to given container!", ex.getMessage());
        }

        System.out.println("Trying to list the directory contents "+containerName+"...");
        try {
            /*
             * List the directory contents.
             */
            directoryClient.listPaths().forEach(path -> System.out.printf("     PathName: %s%n", path.getName()));
            System.out.println("Done Listing!");
        } catch (UncheckedIOException ex) {
            System.err.printf("Failed to upload file %s%n to given container!", ex.getMessage());
        }

        System.out.println("Trying to remove a sample file from the container "+containerName+"...");
        try {
            /*
             * delete the sample file.
             */
            fileClient.deleteIfExists();
            System.out.println("Sample file deletion succeeded!");
        } catch (UncheckedIOException ex) {
            System.err.printf("Failed to delete file %s%n from given container!", ex.getMessage());
        }

        System.out.println("Trying to remove the iwx_test_connectivity directory from the container "+containerName+"...");
        try {
            /*
             * Delete the iwx_test_connectivity directory.
             */
            directoryClient.deleteIfExists();
            System.out.println("Directory iwx_test_connectivity deletion succeeded!");
        } catch (UncheckedIOException ex) {
            System.err.printf("Failed to delete iwx_test_connectivity directory %s%n from given container!", ex.getMessage());
        }
        System.out.println("Testing Complete!");
    }
}
