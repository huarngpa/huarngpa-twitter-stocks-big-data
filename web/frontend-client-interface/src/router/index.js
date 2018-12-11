import Vue from 'vue'
import Router from 'vue-router'
import Home from '@/components/Home'
import Requests from '@/components/Requests'
import TwitterRequests from '@/components/TwitterRequests'
import StockRequests from '@/components/StockRequests'

Vue.use(Router)

export default new Router({
  routes: [
    {
      path: '/',
      name: 'Home',
      component: Home
    },
    {
      path: '/requests',
      name: 'Requests',
      component: Requests
    },
    {
      path: '/requests/twitter',
      name: 'TwitterRequests',
      component: TwitterRequests
    },
    {
      path: '/requests/stock',
      name: 'StockRequests',
      component: StockRequests
    }
  ]
})
