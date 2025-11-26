
import {
    insertToBigQuery,
    realtimeToBigQuery,
    realtimeSession,
    realtimeLocationGuide,

} from "./realtimeFunctions.js";

// Xuất ra tất cả các hàm Cloud Functions để deploy
export {
    insertToBigQuery,
    realtimeToBigQuery,
    realtimeSession,
    realtimeLocationGuide,
};

//--------CLIENT API----
import {getSessions,getSignInAccount,getLocationGuide} from "./client/users.js";
export {
    getSessions,
    getSignInAccount,
    getLocationGuide
};

import {getCompleteReport,getConfirmTour,getRejectTour} from "./client/tour.js";
export {
    getCompleteReport,
    getConfirmTour,
    getRejectTour
};
// todo: data request client api
// {
//     "startDate":"2025-11-18",
//     "endDate":"2025-11-18",
//     "page":1
// }

// TRUNCATE TABLE `project_id.dataset.table_name`; // xóa data in table


//1: cd functions
//2: rm -rf node_modules package-lock.json
//3:  npm install
// 4: cd ..
//5:  firebase deploy --only functions

// firebase deploy --only functions:realtimeSession , realtimeLocationGuide, realtimeSession