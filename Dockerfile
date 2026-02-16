FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY index.js ./

ENV NODE_ENV=production
EXPOSE 8090

CMD ["node", "index.js"]
