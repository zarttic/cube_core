import { createApp } from 'vue';
import ElementPlus from 'element-plus';
import zhCn from 'element-plus/es/locale/lang/zh-cn';
import { createPinia } from 'pinia';
import 'element-plus/dist/index.css';

import App from './App.vue';
import router from './router';
import './styles.css';
import { installErrorReporter } from './api/errorReporter';
import { installGlobalErrorHandlers } from './utils/errorHandler';
import { installOverflowTitles } from './utils/overflowTitle';

const app = createApp(App);
app.use(createPinia()).use(router).use(ElementPlus, { locale: zhCn });
installGlobalErrorHandlers(app);
installErrorReporter();
installOverflowTitles();
app.mount('#app');
