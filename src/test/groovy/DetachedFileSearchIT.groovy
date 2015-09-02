import io.dbmaster.testng.BaseToolTestNGCase;
import io.dbmaster.testng.OverridePropertyNames;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test

import com.branegy.tools.api.ExportType;

@OverridePropertyNames(project="project.detached-file-search")
public class DetachedFileSearchIT extends BaseToolTestNGCase {

    @Test
    @Parameters(["detached-file-search.p_servers","detached-file-search.p_exclude_folders"])
    public void testAll(@Optional String p_servers,
            @Optional("c:\\windows\\winsxs\\") String p_exclude_folders) {
        def parameters = [ "p_servers"  :  p_servers,
                           "p_exclude_folders" :  p_exclude_folders]
        String result = tools.toolExecutor("detached-file-search", parameters).execute()
        assertTrue(result.contains("Results for"), "Unexpected search results ${result}");
    }
}
