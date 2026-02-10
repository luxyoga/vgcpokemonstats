# Miniature Painting Commission Landing Page

A professional, mobile-first landing page for miniature commission painting services.

## Quick Start

1. Open `index.html` in a web browser, or
2. Serve via a local server:
   ```bash
   # Using Python
   python -m http.server 8000
   
   # Using Node.js http-server
   npx http-server
   ```
3. Navigate to `http://localhost:8000`

## Project Structure

```
miniature-painting-site/
├── index.html          # Main landing page
├── styles.css          # Responsive stylesheet
├── app.js              # Interactive functionality
├── images/             # Portfolio images directory
└── README.md           # This file
```

## Features

- **Responsive Design**: Mobile-first approach, works on all devices
- **Service Levels**: Three-tier pricing (Standard, High, Display)
- **Portfolio Gallery**: Filterable gallery with lightbox viewing
- **Process Flow**: Four-step visual guide
- **Testimonials**: Customer review carousel
- **FAQ Section**: Expandable accordion
- **Contact Form**: With validation
- **Blog & Shop**: Basic sections for future expansion

## Customization

### Adding Portfolio Images

1. Add images to the `images/` directory
2. Update the gallery section in `index.html` with your image paths
3. Update the `app.js` gallery data array

### Updating Business Information

- Edit contact information in the contact section
- Update social media links
- Replace placeholder testimonials with real reviews
- Customize service descriptions and pricing

### Styling

- Modify CSS custom properties in `styles.css` for colors and fonts
- Adjust breakpoints in media queries for different screen sizes

## Deployment

This is a static site and can be deployed to:

- **Netlify**: Drag and drop the folder
- **Vercel**: Connect your Git repository
- **GitHub Pages**: Push to a repository and enable Pages
- **Any static hosting**: Upload files to your web server

## Browser Support

- Chrome (latest)
- Firefox (latest)
- Safari (latest)
- Edge (latest)
- Mobile browsers (iOS Safari, Chrome Mobile)

## Future Enhancements

- Backend integration for contact form submissions
- Blog CMS integration
- E-commerce functionality for shop
- Analytics integration
- SEO optimization
