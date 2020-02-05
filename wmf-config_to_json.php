<?php

include "../operations-mediawiki-config/wmf-config/InitialiseSettings.php";

if(function_exists("wmfGetVariantSettings")){
   echo json_encode(wmfGetVariantSettings());
} else if(isset($wgConf)) {
    echo json_encode($wgConf);
}

?>
